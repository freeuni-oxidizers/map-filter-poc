// load data(who loads it?)
// map function(user defined closure/fn)
// filter function(user defined closure/fn)
// result comes back to master and gets printed

// are we okay only supporting non-capturing functions directly

// struct ListRdd<T> {
//     values: Vec<T>
// }

fn main() {
    let values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    // filter all odds
    // double all
    // should get multiples of 4
    let rdd = ListRdd::new(values);
    let result = rdd.filter(|x| x % 2 == 1).map(|x| 2 * x).collect();
    println!("{}", result);
}
