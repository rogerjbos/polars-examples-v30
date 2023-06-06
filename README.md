# polars-examples-v30

This is a collection of 25+ Rust Polars (v.0.30.0) examples taken from the [User Guide](https://pola-rs.github.io/polars-book/user-guide/).  Ironically for a library written in Rust, the examples are more complete in Python.  Being new to Rust and Polars this was a good exercise to get many of the examples working in Rust.  I found that I also had to make consistent changes to the examples to get them to run in my environment, so hopefully users of this code will have a more seamless experience.  Also, the compiler always moved ownership of the df object even though I used clone(), so I created a standalone function for each example.

Obviously, for the `example_sql()` function to work you would need to have to have access to a postgres database, update the connection string, update the query with an existing table.  It should be straightforward to modidy this example for other database connections.

As a bonus, I left in two examples that I could not get to work in case anyone is up for a challenge.  I also could not get any of the [string](https://pola-rs.github.io/polars-book/user-guide/expressions/strings/) examples to run, so I didn't bother to include them.  Maybe I am missing a feature?

Pull requests are welcome for any fixes or code improvements.  I hope these examples help you.