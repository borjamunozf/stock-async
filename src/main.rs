mod finance;

use async_std::stream::{self, StreamExt};
use chrono::prelude::*;
use clap::Parser;
use std::{
    fs::File,
    io::{BufWriter, Error, ErrorKind, Write},
    time::Duration,
};
use xactor::{message, Actor, Broker, Context, Handler, Service, Supervisor};
use yahoo::{time::OffsetDateTime, YahooConnector};
use yahoo_finance_api as yahoo;

use finance::AsyncStockSignal;

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Borja Muñoz",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

struct DownloadActor {
    provider: YahooConnector,
}

struct FinanceDataActor {}

#[derive(Clone)]
struct PrintActor {}

struct WriterActor {
    filename: String,
    writer: Option<BufWriter<File>>,
}

/// Message for DownloadActor to communicate stock symbols & timeframe
//#[message(result = "std::io::Result<Vec<f64>>")]
#[derive(Clone)]
#[message]
struct StockDataRequest {
    symbol: String,
    from: OffsetDateTime,
    to: OffsetDateTime,
}

/// Message for FinanceActor to get finance stats for symbol
//#[message(result = "Option<SymbolFinanceResponse>")]
#[derive(Clone)]
#[message]
struct SymbolFinanceRequest {
    series: Vec<f64>,
    symbol: String,
    from: OffsetDateTime,
}

#[derive(Clone)]
#[message]
struct SymbolFinanceResponse {
    symbol: String,
    last_price: f64,
    pct_change: f64,
    period_min: f64,
    period_max: f64,
    sma: Vec<f64>,
}

#[message]
#[derive(Clone)]
struct PrintRequest {
    symbol_data: SymbolFinanceResponse,
    from: OffsetDateTime,
}

#[message]
#[derive(Clone)]
struct WriteRequest {
    symbol_data: SymbolFinanceResponse,
    from: OffsetDateTime,
}

#[async_trait::async_trait]
impl Actor for FinanceDataActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> xactor::Result<()> {
        ctx.subscribe::<SymbolFinanceRequest>().await
    }
}

#[async_trait::async_trait]
impl Actor for DownloadActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> xactor::Result<()> {
        self.provider = YahooConnector::new();
        ctx.subscribe::<StockDataRequest>().await
    }
}

#[async_trait::async_trait]
impl Handler<StockDataRequest> for DownloadActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: StockDataRequest) {
        let provider = yahoo::YahooConnector::new();
        let series: Vec<f64> = match provider
            .get_quote_history(&msg.symbol, msg.from, msg.to)
            .await
        {
            Ok(response) => {
                if let Ok(mut quotes) = response.quotes() {
                    quotes.sort_by_cached_key(|k| k.timestamp);
                    quotes.iter().map(|q| q.adjclose as f64).collect()
                } else {
                    vec![]
                }
            }
            Err(e) => {
                eprintln!("Ignoring API error for symbol '{}': {}", &msg.symbol, e);
                vec![]
            }
        };

        println!("que facemos");
        if let Err(e) = Broker::from_registry()
            .await
            .unwrap()
            .publish(SymbolFinanceRequest {
                series,
                symbol: msg.symbol,
                from: msg.from,
            })
        {
            eprintln!("Failed to publish symbol finance req stats: {}", e);
        }
    }
}

#[async_trait::async_trait]
impl Handler<SymbolFinanceRequest> for FinanceDataActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: SymbolFinanceRequest) {
        println!("QUE FACEMOS FINANCE DATA");

        // signals types
        let max_signal = finance::MaxPrice {};
        let min_signal = finance::MinPrice {};
        let pct_diff = finance::PriceDifference {};
        let window_sma = finance::WindowedSMA { window_size: 30 };
        if !msg.series.is_empty() {
            // min/max of the period. unwrap() because those are Option types
            let period_max = max_signal.calculate(&msg.series).await.unwrap();
            let period_min = min_signal.calculate(&msg.series).await.unwrap();
            let last_price = *msg.series.last().unwrap_or(&0.0);
            let (_, pct_change) = pct_diff.calculate(&msg.series).await.unwrap();
            let sma = window_sma.calculate(&msg.series).await.unwrap();
            if let Err(e) = Broker::from_registry()
                .await
                .unwrap()
                .publish(PrintRequest {
                    symbol_data: SymbolFinanceResponse {
                        symbol: msg.symbol,
                        last_price,
                        pct_change,
                        period_min,
                        period_max,
                        sma,
                    },
                    from: msg.from,
                })
            {
                eprintln!("Failed to send symbol finance data {}", e);
            }
        }
    }
}

#[async_trait::async_trait]
impl Actor for PrintActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> xactor::Result<()> {
        ctx.subscribe::<PrintRequest>().await
    }
}

#[async_trait::async_trait]
impl Handler<PrintRequest> for PrintActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: PrintRequest) {
        // a simple way to output CSV data
        println!(
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
            msg.from.time(),
            msg.symbol_data.symbol,
            msg.symbol_data.last_price,
            msg.symbol_data.pct_change,
            msg.symbol_data.period_min,
            msg.symbol_data.period_max,
            msg.symbol_data.sma.last().unwrap_or(&0.0),
        );
    }
}

#[async_trait::async_trait]
impl Actor for WriterActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> xactor::Result<()> {
        let mut file =
            File::create(&self.filename).unwrap_or_else(|_| panic!("Could not open file"));

        let _ = writeln!(&mut file, "period,symbol,price,change%,min,max,30d-avg");

        self.writer = Some(BufWriter::new(file));
        ctx.subscribe::<WriteRequest>().await
    }

    async fn stopped(&mut self, ctx: &mut Context<Self>) {
        if let Some(writer) = &mut self.writer {
            writer
                .flush()
                .expect("Something happened when flushing. Data loss :(")
        };
        ctx.stop(None);
    }
}

#[async_trait::async_trait]
impl Handler<WriteRequest> for WriterActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: WriteRequest) {
        // a simple way to output CSV data
        println!(
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
            msg.from.time(),
            msg.symbol_data.symbol,
            msg.symbol_data.last_price,
            msg.symbol_data.pct_change,
            msg.symbol_data.period_min,
            msg.symbol_data.period_max,
            msg.symbol_data.sma.last().unwrap_or(&0.0),
        );
    }
}

///
/// Calculates the absolute and relative difference between the beginning and ending of an f64 series. The relative difference is relative to the beginning.
///
/// # Returns
///
/// A tuple `(absolute, relative)` difference.
///

///
/// Window function to create a simple moving average
///

///
/// Find the maximum in a series of f64
///

///
/// Find the minimum in a series of f64
///

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &OffsetDateTime,
    end: &OffsetDateTime,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();

    let response = provider
        .get_quote_history(symbol, *beginning, *end)
        .await
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;

    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}

async fn get_symbol_data(
    symbol: &str,
    from: &OffsetDateTime,
    to: &OffsetDateTime,
) -> Option<Vec<f64>> {
    // signals types
    let max_signal = finance::MaxPrice {};
    let min_signal = finance::MinPrice {};
    let pct_diff = finance::PriceDifference {};
    let window_sma = finance::WindowedSMA { window_size: 30 };
    let closes = fetch_closing_data(&symbol, &from, &to).await.ok()?;
    if !closes.is_empty() {
        // min/max of the period. unwrap() because those are Option types
        let period_max = max_signal.calculate(&closes).await.unwrap();
        let period_min = min_signal.calculate(&closes).await.unwrap();
        let last_price = *closes.last().unwrap_or(&0.0);
        let (_, pct_change) = pct_diff.calculate(&closes).await.unwrap();
        let sma = window_sma.calculate(&closes).await.unwrap();

        // a simple way to output CSV data
        println!(
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
            from.time(),
            symbol,
            last_price,
            pct_change * 100.0,
            period_min,
            period_max,
            sma.last().unwrap_or(&0.0)
        );
        return Some(closes);
    }

    None
}

#[xactor::main]
async fn main() -> xactor::Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let from: OffsetDateTime = OffsetDateTime::from_unix_timestamp(from.timestamp()).unwrap();
    let mut to = OffsetDateTime::now_utc();

    // store all symbols from file
    let mut symbols = "";
    if opts.symbols.is_empty() {
        symbols = include_str!("../sp500.dec.2022.txt");
    } else {
        symbols = &opts.symbols;
    }

    // init actors
    let _downloader = Supervisor::start(|| DownloadActor {
        provider: YahooConnector::new(),
    })
    .await;
    let _finance_actor = Supervisor::start(|| FinanceDataActor {}).await;
    let _print_act = Supervisor::start(|| PrintActor {}).await;
    let _writer_act = Supervisor::start(|| WriterActor {
        filename: format!("{}.csv", Utc::now().to_rfc2822()), // create a unique file name every time
        writer: None,
    })
    .await;

    let mut interval = stream::interval(Duration::from_secs(30));
    println!("period start,symbol,price,change %,min,max,30d avg");
    let symbols: Vec<&str> = symbols.split(",").to_owned().collect();
    while let Some(_) = interval.next().await {
        for symbol in &symbols {
            if let Err(e) = Broker::from_registry().await?.publish(StockDataRequest {
                symbol: symbol.to_string(),
                from,
                to,
            }) {
                eprintln!("{}", e);
                break;
            }
        }
        to = OffsetDateTime::now_utc();
    }
    Ok(())
}
