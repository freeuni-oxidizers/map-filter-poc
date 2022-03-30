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



pub trait Rdd {
    type Item;

    fn collect(self) -> Vec<Self::Item>;

    fn map<B>(self, fp: fn(Self::Item) -> B) -> MapRdd<Self, fn(Self::Item) -> B>
    where
        Self: Sized,
    {
        MapRdd { prev: self, f: fp }
    }
}

fn main() {
    // filter all odds
    // double all
    // should get multiples of 4

    let values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].to_vec();
    let rdd = ListRdd::new(values);
    let r = rdd.map(|x|2*x);
    dbg!(r.collect());
    
    // rddmapfiler(values, |x|2*x, |x|x%2==1)

    // let result = rdd.filter(|x| x % 2 == 1).map(|x| 2 * x).collect();

    // println!("{}", result);
}
