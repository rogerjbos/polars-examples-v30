#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
#[macro_use] extern crate polars_core;
use polars_core::prelude::*;
use polars_lazy::prelude::*;
use polars::prelude::*;
use reqwest::blocking::Client;

use connectorx::prelude::*;
use std::convert::TryFrom;
use chrono::prelude::*;
use rand::{thread_rng, Rng};
use std::error::Error;
use polars::{df, prelude::*};
use std::env;
use std::fs::File;
use polars_io::avro::AvroReader;
use polars_io::avro::AvroWriter;
use polars_io::SerReader;
use polars_io::SerWriter;

use connectorx::{
    constants::RECORD_BATCH_SIZE,
    destinations::arrow2::Arrow2Destination,
    prelude::*,
    sources::{postgres::{rewrite_tls_args, BinaryProtocol, PostgresSource},},
    sql::CXQuery,
};
use std::ops::Add;
use std::ops::Mul;

fn main() {

    example_select_all();
    example_drop_columns();
    example_unique();
    example_numerical();
    example_lazy_filter_agg();
    example_lazy_agg();
    example_context();
    example_groupby();
    example_dates_csv();
    example_parquet();
    example_avro();
    example_piped();
    example_sql();
    example_conditional();
    example_casting();
    example_reverse_add();
    example_custom_fn();
    example_join();
    example_fill_na();
    example_over();
    example_filtered_sort();
    example_fold();
    example_concat_str();
    example_groupby_apply();
    example_combining_multiple_col_values();
    // example_flatten_not_working();
    // example_filtered_fold_not_working();
   
}

pub fn example_combining_multiple_col_values() {
    let df = df!(
        "keys" => &["a", "a", "a", "a", "a", "b", "b", "b"],
        "values" => &[1, 2, 3, 4, 5, 11, 12, 13],
    );


    let out = df.unwrap()
        .lazy()
        .select([
            // pack to struct to get access to multiple fields in a custom `apply/map`
            as_struct(&[col("keys"), col("values")])
                // we will compute the len(a) + b
                .apply(
                    |s| {
                        // downcast to struct
                        let ca = s.struct_()?;

                        // get the fields as Series
                        let s_a = &ca.fields()[0];
                        let s_b = &ca.fields()[1];

                        // downcast the `Series` to their known type
                        let ca_a = s_a.utf8()?;
                        let ca_b = s_b.i32()?;

                        // iterate both `ChunkedArrays`
                        let out: Int32Chunked = ca_a
                            .into_iter()
                            .zip(ca_b)
                            .map(|(opt_a, opt_b)| match (opt_a, opt_b) {
                                (Some(a), Some(b)) => Some(a.len() as i32 + b),
                                _ => None,
                            })
                            .collect();

                        Ok(Some(out.into_series()))
                    },
                    GetOutput::from_type(DataType::Int32),
                )
                .alias("solution_apply"),
            (col("keys").str().count_match(".") + col("values")).alias("solution_expr"),
        ])
        .collect();
    println!("Combining multiple column values: {:?}", out);
}

pub fn example_groupby_apply() {
    let df = df!(
        "keys" => &["a", "a", "a", "a", "a", "b", "b", "b"],
        "values" => &[1, 2, 3, 4, 5, 11, 12, 13],
    );
    
    // So my advice is to never use map in the groupby context unless you know you need it and know what you are doing.
    let out = df.unwrap()
        .clone()
        .lazy()
        .groupby([col("keys")])
        .agg([
            col("values")
                .apply(|s| Ok(Some(s.shift(1))), GetOutput::default())
                .alias("shift_map"),
            col("values").shift(1).alias("shift_expression"),
        ])
        .collect();
    
    println!("shift: {:?}", out);

}

pub fn example_concat_str() {
    let df = df!(
        "a" => &["a", "b", "c"],
        "b" => &[1, 2, 3],
    );
    
    let out = df.unwrap()
        .lazy()
        .select([concat_str([col("a"), col("b")], "")])
        .collect();
    println!("fold string: {:?}", out);
}

// https://pola-rs.github.io/polars-book/user-guide/expressions/window/#more-examples
// pub fn example_flatten_not_working() {
//     let data: Vec<u8> = Client::new()
//     .get("https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv")
//     .send().unwrap()
//     .text().unwrap()
//     .bytes()
//     .collect();

//     let df = CsvReader::new(std::io::Cursor::new(data))
//         .has_header(true)
//         .finish();
//     println!("pokemon: {:?}", df);

//     let out = df.unwrap()
//         .clone()
//         .lazy()
//         .select([
//             col("Type 1")
//                 .head(Some(3))
//                 .list()
//                 .0.over(["Type 1"])
//                 .flatten(),
//             col("Name")
//                 .sort_by(["Speed"], [false])
//                 .head(Some(3))
//                 .list()
//                 .0.over(["Type 1"])
//                 .flatten()
//                 .alias("fastest/group"),
//             col("Name")
//                 .sort_by(["Attack"], [false])
//                 .head(Some(3))
//                 .list()
//                 .0.over(["Type 1"])
//                 .flatten()
//                 .alias("strongest/group"),
//             col("Name")
//                 .sort(false)
//                 .head(Some(3))
//                 .list()
//                 .0.over(["Type 1"])
//                 .flatten()
//                 .alias("sorted_by_alphabet"),
//         ])
//         .collect();
//     println!("flatten: {:?}", out);
// }

// https://pola-rs.github.io/polars-book/user-guide/expressions/folds/#conditional
// pub fn example_filtered_fold_not_working() {
//     let df = df!(
//         "a" => &[1, 2, 3],
//         "b" => &[0, 1, 2],
//     );
//     let out = df.unwrap()
//         .lazy()
//         .filter(fold_exprs(lit(true), |acc, x| Ok(Some(acc.bitand(&x))), [col("*").gt(1)]).alias("sum"))
//         .collect();
//     println!("filtered fold: {:?}", out);
// }

pub fn example_fold() {
    let df = df!(
        "a" => &[1, 2, 3],
        "c" => &[1, 2, 3],
        "b" => &[10, 20, 30],
        "d" => &[10, 20, 30],
    );
    
    let out = df.unwrap()
        .lazy()
        .select([fold_exprs(lit(0), |acc, x| Ok(Some(acc + x)), [col("a"), col("b"), col("c")]).alias("sum")])
        .collect();
    println!("fold: {:?}", out);
}

pub fn example_over() {
    let data: Vec<u8> = Client::new()
    .get("https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv")
    .send().unwrap()
    .text().unwrap()
    .bytes()
    .collect();

    let df = CsvReader::new(std::io::Cursor::new(data))
        .has_header(true)
        .finish();
    println!("pokemon: {:?}", df);

    let out = df.unwrap()
        .clone()
        .lazy()
        .select([
            col("Type 1"),
            col("Type 2"),
            col("Attack")
                .mean()
                .over(["Type 1"])
                .alias("avg_attack_by_type"),
            col("Defense")
                .mean()
                .over(["Type 1", "Type 2"])
                .alias("avg_defense_by_type_combination"),
            col("Attack").mean().alias("avg_attack"),
        ])
        .collect();
    println!("over: {:?}", out);
}

pub fn example_filtered_sort() {
    let data: Vec<u8> = Client::new()
        .get("https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv")
        .send().unwrap()
        .text().unwrap()
        .bytes()
        .collect();

    let df = CsvReader::new(std::io::Cursor::new(data))
        .has_header(true)
        .finish();
    println!("pokemon: {:?}", df);

    let filtered = df.unwrap()
        .clone()
        .lazy()
        .filter(col("Type 2").eq(lit("Psychic")))
        .select([col("Name"), col("Type 1"), col("Speed")])
        .collect();

    println!("filtered not sorted: {:?}", filtered);

    let out = filtered.unwrap()
        .lazy()
        .with_columns([cols(["Name", "Speed"]).sort_by(["Speed"],[true]).over(["Type 1"])])
        .collect();
    println!("filtered sort: {:?}", out);
}

pub fn example_fill_na() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );
    println!("random DF: {:?}", &df);

    let new = df.unwrap().lazy()
        .with_column(
            col("nrs")
            .fill_null(lit(0)),
            // .fill_null(median("nrs")),
            // .interpolate(), //??
            // .fill_null(strategy="forward"), //??
        )
        .mean()
        .collect()
        .unwrap();
    println!("fill na mean: {:?}", new);
}

pub fn example_join() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df_a = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );
    println!("random DF: {:?}", &df_a);

    let df_b = df! (
        "numbers" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "more_names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), Some("test")],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let out = df_a.unwrap().lazy()
        .left_join(df_b.unwrap().lazy(), col("names"), col("more_names"))
        .filter(
            col("numbers").lt(lit(4))
        )
        .groupby([col("groups")])
        .agg(
            vec![col("nrs").first(), col("numbers").first()]
        )
        .select(&[col("groups"), col("nrs")])
        .collect();
    println!("join: {:?}", out);
}

pub fn example_custom_fn() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );
    println!("random DF: {:?}", &df);

    let new = df.unwrap().lazy()
        .with_column(
            col("random")
            // apply a custom closure Series => Result<Series>
            .map(|_s| {
                Ok(Some(Series::new("test_col", &[6.0f64, 6.0, 6.0, 6.0, 6.0])))
            },
            // return type of the closure - type change doesn't work
            GetOutput::from_type(DataType::Utf8)).alias("new_column")
        )
        .collect()
        .unwrap();
    println!("new: {:?}", new);
}

pub fn example_reverse_add() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );
    println!("random DF: {:?}", &df);

    let new = df.unwrap().lazy()
        // Note the reverse here!!
        .reverse()
        .with_column(
            (col("random") * lit(0.5)).alias("random * 0.5"),
        )
        .with_column(
            (col("random") * lit(2)).alias("random * 2"),
        )
        .with_column(
            (col("random") * lit(3)).alias("random * 3"),
        )
        .with_column(
            (col("random") * lit(4)).alias("random * 4"),
        )
        .collect()
        .unwrap();
    println!("new: {:?}", new);
}

pub fn example_avro() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );
    println!("random DF: {:?}", &df);

    let mut file = File::create("output.avro").expect("couldn't create file");
    AvroWriter::new(&mut file).finish(&mut df.unwrap()).expect("couldn't write file");

    let file = File::open("output.avro").expect("file not found");
    let df_avro = AvroReader::new(file)
        .finish();
    println!("df_avro: {:?}", df_avro);
}

pub fn example_parquet() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );
    println!("random DF: {:?}", &df);

    let mut file = File::create("output.parquet").expect("could not create file");
    ParquetWriter::new(&mut file).finish(&mut df.unwrap()).expect("couldn't write file");

    let df_parquet = LazyFrame::scan_parquet("output.parquet", ScanArgsParquet::default()).unwrap()
        .select([
            all(),
            // and do some aggregations
            cols(["nrs", "random"]).sum().suffix("_summed"),
        ])
        .collect();
    println!("df_parquet: {:?}", df_parquet);
}

pub fn example_casting() {
    let df = df! (
        "integers" => &[Some(1), Some(2), Some(3), Some(4), Some(5)],
        "big_integers" => &[Some(1), Some(10000002), Some(3), Some(10000004), Some(10000005)],
        "floats" => &[Some(4.0), Some(5.0), Some(6.0), Some(7.0), Some(8.0)],
        "floats_as_string" => &["4.0".to_string(), "5.0".to_string(), "6.0".to_string(), "7.0".to_string(), "8.0".to_string()],
        "floats_with_decimals" => &[Some(4.532), Some(5.6), Some(6.7), Some(7.8), Some(8.9)],
        "bools" => &[true, false, true, false, true],
    );
    println!("df: {:?}", &df);
    
    let out = df.unwrap().clone().lazy().select([
        col("integers").cast(DataType::Float32).alias("integers_as_floats"),
        col("floats").cast(DataType::Int32).alias("floats_as_integers"),
        col("floats_with_decimals").cast(DataType::Int32).alias("floats_with_decimal_as_integers"),
        col("integers").cast(DataType::Utf8).alias("integers_as_str"),
        col("floats").cast(DataType::Utf8).alias("floats_as_str"),
        col("floats_as_string").cast(DataType::Float64).alias("floats_with_decimal_as_float"),
        col("integers").cast(DataType::Boolean).alias("integers_as_boolean"),
        col("bools").cast(DataType::Boolean).alias("bools_as_integers"),
        // col("floats").cast(DataType::Float32).alias("floats_smallfootprint"), //??
        // col("big_integers").cast(DataType::Int64).alias("big_integers_smallfootprint"), //??
     ]).collect();
    println!("casting: {:?}", out);
}

pub fn example_context() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let out = df.unwrap()
    .clone()
    .lazy()
    .select([
        sum("nrs"),
        col("names").sort(false),
        col("names").first().alias("first name"),
        (mean("nrs") * lit(10)).alias("10xnrs"),
    ])
    .collect();
    println!("context: {:?}", out);
}

pub fn example_groupby() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let out = df.expect("groupby")
        .lazy()
        .groupby([col("groups")])
        .agg([
            sum("nrs"),                           // sum nrs by groups
            col("random").count().alias("count"), // count group members
            // sum random where name != null
            col("random")
                .filter(col("names").is_not_null())
                .sum()
                .suffix("_sum"),
            col("names").reverse().alias("reversed names"),
        ])
        .collect();
    println!("groupby: {:?}", out);
}

pub fn example_lazy_filter_agg() {
    let df = CsvReader::from_path("iris.csv").unwrap().finish().unwrap();
    println!("iris: {:?}", df);
    
    let mask = df.column("sepal_width").unwrap().f64().unwrap().gt(3.1);
    let df_small = df.filter(&mask);
    let df_agg = df_small.expect("REASON").lazy()
        .groupby_stable([col("species")])
        .agg([
            col("sepal_width").mean(),
        ])
        .collect();
    println!("lazy filter agg: {:?}", df_agg);
}

pub fn example_lazy_agg() {
    let q = LazyCsvReader::new("iris.csv")
        .has_header(true)
        .finish().unwrap()
        .filter(col("sepal_length").gt(lit(5)))
        .groupby(vec![col("species")])
        .agg([col("sepal_width").mean()]);
    // without streaming
    // let df = q.collect();
    // with streaming
    let df = q.with_streaming(true).collect();
    println!("lazy agg: {:?}", df);
}

pub fn example_piped() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let out = df.unwrap().clone().lazy().select([
        col("names").sort(Default::default()).head(Some(20)),
        col("nrs").filter(col("nrs").eq(lit(1))).sum(),
     ]).collect();
    println!("piped: {:?}", out);
}

pub fn example_sql() {
    let pg = env::var("PG").unwrap();
    let conn = String::from(format!("postgresql://postgres:{pg}@192.168.86.68/tiingo?cxprotocol=binary"));
    let source_conn = SourceConn::try_from(&*conn).expect("parse conn str failed");
    
    let queries = &[CXQuery::from("SELECT * FROM price_history_old limit 10")];
    
    let destination = get_arrow2(&source_conn, None, queries).expect("run failed");

    let data = destination.polars();
    println!("SQL: {:?}", data);
}

pub fn example_dates_csv() {
    let mut df: DataFrame = df!("integer" => &[1, 2, 3, 4, 5],
    "date" => &[
                NaiveDate::from_ymd_opt(2022, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
                NaiveDate::from_ymd_opt(2022, 1, 2).unwrap().and_hms_opt(0, 0, 0).unwrap(),
                NaiveDate::from_ymd_opt(2022, 1, 3).unwrap().and_hms_opt(0, 0, 0).unwrap(),
                NaiveDate::from_ymd_opt(2022, 1, 4).unwrap().and_hms_opt(0, 0, 0).unwrap(),
                NaiveDate::from_ymd_opt(2022, 1, 5).unwrap().and_hms_opt(0, 0, 0).unwrap()
    ],
    "float" => &[4.0, 5.0, 6.0, 7.0, 8.0]
    ).unwrap();
    println!("df: {}", df);
    println!("head: {}", df.head(Some(3)));
    println!("sample: {:?}",df.sample_n(2, false, true, None));
    
    let mut file = File::create("output.csv").expect("could not create file");
    CsvWriter::new(&mut file).has_header(true).with_delimiter(b',').finish(&mut df).expect("couldn't write file");

    let df_csv2 = CsvReader::from_path("output.csv").unwrap().infer_schema(None).has_header(true).with_try_parse_dates(true).finish();
    println!("Dates as datetime: {:?}", df_csv2);  

}

pub fn example_numerical() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let out = df.unwrap()
        .clone()
        .lazy()
        .select([
            (col("nrs") + col("random")).alias("nrs + random"),
            (col("nrs") - col("random")).alias("nrs - random"),
            (col("nrs") * col("random")).alias("nrs * random"),
            (col("nrs") / col("random")).alias("nrs / random"),
            (col("nrs").eq(lit(1))).alias("nrs == 1"),
            (col("nrs").neq(lit(1))).alias("nrs != 1"),
            (col("nrs").gt(lit(1))).alias("nrs > 1"),
            (col("nrs").lt(lit(1))).alias("nrs < 1"),
            (col("random") + lit(5.0)).alias("random + 5"),
            (col("random") - lit(5.0)).alias("random - 5"),
            (col("random") * lit(5.0)).alias("random * 5"),
            (col("random") / lit(5.0)).alias("random / 5"),
        ])
        .collect();
    println!("numerical: {:?}", out);
}

pub fn example_drop_columns() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let out = df.unwrap().clone().lazy().drop_columns(["random","groups"]).collect();
    println!("drop columns: {:?}", out);
}

pub fn example_select_all() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let out = df.unwrap().clone().lazy().select([col("*")]).collect();
    // let out = df.unwrap().clone().lazy().select([all()]).collect();
    println!("all: {:?}", out);
}

pub fn example_unique() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let out = df.unwrap().clone().lazy().select([
        col("names").n_unique().alias("unique"),
        // approx_unique("names").alias("unique_approx")
        ]).collect();
    println!("unique: {:?}", out);
}

pub fn example_conditional() {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);
    
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    );

    let conditional= df.unwrap().lazy()
    .groupby_stable([col("groups")])
    .agg( [
        external_func().alias("Weight"),
    ]
    ).collect();
    println!("conditional: {:?}", conditional);
}

pub fn external_func() -> Expr {
    when(col("nrs").gt(lit(1.0)))
    .then(lit(true))
    .otherwise(lit(false))
}

