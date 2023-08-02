use actix::{Actor, Message, Context, Handler};
use async_trait::async_trait;
use chrono::prelude::*;
use clap::Parser;
use futures::future;
use tokio::{stream, time};
use tokio_stream::{wrappers::IntervalStream, StreamExt};
use std::{io::{Error, ErrorKind}, time::{Duration, Instant}};
use yahoo::{time::OffsetDateTime, YahooConnector};
use yahoo_finance_api as yahoo;

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
    provider: YahooConnector
}

struct FinanceDataActor {

}

struct PrintActor {

}

struct WriterActor {

}

/// Message for DownloadActor to communicate stock symbols & timeframe
#[derive(Message)]
#[rtype(result = "Result<Vec<f64>>, std::io::Error")]
struct TargetData {
    symbol: String,
    from: OffsetDateTime,
    to: OffsetDateTime,
}

impl Actor for FinanceDataActor {
    type Context = Context<Self>;
}

impl Actor for DownloadActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        self.provider = yahoo::YahooConnector::new();
    }   
}

impl Handler<TargetData> for DownloadActor {
    type Result = Result<Vec<f64>, std::io::Error>;

    fn handle(&mut self, msg: TargetData, ctx: &mut Self::Context) -> Self::Result {
        let response = self.provider
            .get_quote_history(&msg.symbol, msg.from, msg.to)
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
}

impl Actor for PrintActor {
    type Context = Context<Self>;
}

impl Actor for WriterActor {
    type Context = Context<Self>;
}

///
/// A trait to provide a common interface for all signal calculations.
///
#[async_trait]
trait AsyncStockSignal {
    ///
    /// The signal's data type.
    ///
    type SignalType;

    ///
    /// Calculate the signal on the provided series.
    ///
    /// # Returns
    ///
    /// The signal (using the provided type) or `None` on error/invalid data.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

#[derive(Debug)]
struct PriceDifference {}

#[derive(Debug, PartialEq)]
struct MinPrice {}

#[derive(Debug, PartialEq)]
struct MaxPrice {}

#[derive(Debug, PartialEq)]
struct WindowedSMA {
    window_size: u32,
}

#[async_trait]
impl AsyncStockSignal for MinPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            return None;
        }
        Some(series.iter().fold(f64::MAX, |acc, q: &f64| acc.min(*q)))
    }
}
#[async_trait]
impl AsyncStockSignal for MaxPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            return None;
        }

        Some(series.iter().fold(f64::MIN, |acc, q: &f64| acc.max(*q)))
    }
}

#[async_trait]
impl AsyncStockSignal for PriceDifference {
    type SignalType = (f64, f64);

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            return None;
        }
        let (first, last) = (series.first().unwrap(), series.last().unwrap());
        let abs_diff = last - first;
        let first = if *first == 0.0 { 1.0 } else { *first };
        let rel_diff = abs_diff / first;
        Some((abs_diff, rel_diff))
    }
}

#[async_trait]
impl AsyncStockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() && self.window_size > 1 {
            return None;
        }

        Some(
            series
                .windows(self.window_size as usize)
                .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                .collect(),
        )
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
    let max_signal = MaxPrice {};
    let min_signal = MinPrice {};
    let pct_diff = PriceDifference {};
    let window_sma = WindowedSMA { window_size: 30 };
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
        return Some(closes)   
    }

    None
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let from: OffsetDateTime = OffsetDateTime::from_unix_timestamp(from.timestamp()).unwrap();
    let to = OffsetDateTime::now_utc();

    // store all symbols from file
    let mut symbols = "";
    if opts.symbols.is_empty() {
        symbols = include_str!("../sp500.dec.2022.txt");
    } else {
        symbols = &opts.symbols;
    }
    
    // start download actors
    let download_actor = DownloadActor {};
    let addr_download = download_actor.start();

    let mut interval = IntervalStream::new(time::interval(Duration::from_secs(30)));
    println!("period start,symbol,price,change %,min,max,30d avg");
    let symbols: Vec<&str> = symbols.split(",").collect();
    while let Some(_) = interval.next().await {
        let queries: Vec<_> = symbols.iter()
        .map(|symbol| get_symbol_data(symbol, &from, &to))
        .collect();

        let _ = future::join_all(queries).await;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    use super::*;

    #[tokio::test]
    async fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some((1.0, 1.0))
        );
    }

    #[tokio::test]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(0.0)
        );
    }

    #[tokio::test]
    async fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(1.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(6.0)
        );
    }

    #[tokio::test]
    async fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series).await,
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series).await, Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series).await, Some(vec![]));
    }
}
