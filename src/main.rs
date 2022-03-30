use std::fmt::Debug;
use std::io::{prelude::*, BufReader};

// load data(who loads it?)
// map function(user defined closure/fn)
// filter function(user defined closure/fn)
// result comes back to master and gets printed

// are we okay only supporting non-capturing functions directly

struct ListRdd<T> {
    values: Vec<T>,
}

impl<T> Rdd for ListRdd<T> {
    type Item = T;

    fn collect(self) -> Vec<Self::Item> {
        self.values
    }
}

impl<T> ListRdd<T> {
    fn new(values: Vec<T>) -> Self {
        Self { values }
    }
}

pub struct MapRdd<R, F> {
    prev: R,
    f: F,
}

impl<U, R: Rdd> Rdd for MapRdd<R, fn(R::Item) -> U> {
    type Item = U;

    fn collect(self) -> Vec<Self::Item> {
        self.prev.collect().into_iter().map(self.f).collect()
    }
}

pub struct FilterRdd<R, F> {
    prev: R,
    f: F,
}

impl<R: Rdd> Rdd for FilterRdd<R, fn(&R::Item) -> bool> {
    type Item = R::Item;

    fn collect(self) -> Vec<Self::Item> {
        self.prev.collect().into_iter().filter(self.f).collect()
    }
}

pub trait Rdd {
    type Item;

    fn collect(self) -> Vec<Self::Item>;

    fn collect_dist(self) -> Option<Vec<Self::Item>>
    where
        Self: Sized,
        <Self as Rdd>::Item: serde::ser::Serialize,
        <Self as Rdd>::Item: serde::de::DeserializeOwned,
        <Self as Rdd>::Item: Debug,
    {
        let args = Args::parse();
        if args.master {
            // receive results from worker
            let listener = TcpListener::bind("127.0.0.1:1783").unwrap();
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let mut buf = Vec::new();
                let mut bufreader = BufReader::new(stream);
                bufreader.read_to_end(&mut buf).unwrap();
                let serialized = String::from_utf8(buf).unwrap();
                let deserialized: Vec<Self::Item> = serde_json::from_str(&serialized).unwrap();
                dbg!(deserialized);
            }

            None
        } else {
            let results = self.collect();
            let serialized = serde_json::to_string(&results).unwrap();
            dbg!(&serialized);

            let mut stream = TcpStream::connect("127.0.0.1:1783").unwrap();
            stream.write(&serialized.as_bytes()).unwrap();

            None
        }
    }

    fn map<B>(self, fp: fn(Self::Item) -> B) -> MapRdd<Self, fn(Self::Item) -> B>
    where
        Self: Sized,
    {
        MapRdd { prev: self, f: fp }
    }

    fn filter(self, fp: fn(&Self::Item) -> bool) -> FilterRdd<Self, fn(&Self::Item) -> bool>
    where
        Self: Sized,
    {
        FilterRdd { prev: self, f: fp }
    }
}

use std::net::{TcpListener, TcpStream};

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Run as master
    #[clap(long)]
    master: bool,
}

// filter all odds
// double all
// should get multiples of 4
fn main() {
    let values: Vec<i32> = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].to_vec();
    let rdd = ListRdd::new(values);
    let r = rdd.filter(|x| x % 2 == 1).map(|x| 2 * x);
    dbg!(r.collect_dist());
}
