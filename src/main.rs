use error_chain::error_chain;
use tokio::time::{sleep, Duration};
use futures::{stream, StreamExt}; 
use tokio;
use tokio_postgres::NoTls;

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
    }
}

async fn parallel_requests(url: &str) -> Result<()>{
    let concurrent_requests = 2;
    let client = reqwest::Client::new();

    let urls = vec![url; 2];

    let bodies = stream::iter(urls)
        .map(|url| {
            let client = &client;
            async move {
                let resp = client.get(url).send().await?;
                resp.bytes().await
            }
        })
        .buffer_unordered(concurrent_requests);

    bodies
        .for_each(|b| async {
            match b {
                Ok(b) => println!("got {} bytes", b.len()),
                Err(e) => eprintln!("got an error: {}", e),
            }
        })
        .await;
    Ok(())
}

async fn sequential_requests(url: &str) -> Result<()>{
    let client = reqwest::Client::new();
    let num_requests = 100;
    let interval = Duration::from_secs(60) / num_requests;

    let start_time = tokio::time::Instant::now();

    for i in 0..num_requests {
        let cycle_start_time = tokio::time::Instant::now();
        let url = format!("{}", url);

        let response = client.get(url).send().await?;
        let status = response.status();
        //let body = response.text().await?;  //this line prints out the html body of the url we are hitting

        println!("request {}: status: {}", i + 1, status);

        let pre_sleep_cycle_elapsed = cycle_start_time.elapsed();
        println!("time taken in one cycle before sleep {:?}", pre_sleep_cycle_elapsed);

        sleep(interval).await;
        let elapsed = start_time.elapsed();
        let cycle_elapsed = cycle_start_time.elapsed();
        println!("total time elapsed: {:?}", elapsed);
        println!("time taken in one cycle {:?}\n", cycle_elapsed);
    }
    let elapsed = start_time.elapsed();
    println!("total time: {:?}", elapsed);

    Ok(())
}

async fn post_request_with_form(userid: &str, name: &str, url: &str) -> Result<()>{
    let params = [("userid", userid), ("name", name)];
    let client = reqwest::Client::new();
    let res = client.post(url).form(&params).send().await?;
    println!("status: {}", res.status());
    println!("headers:\n{:#?}", res.headers());

    //let body = res.text().await?;
    //println!("body:\n{}", body); 

    Ok(())
}

async fn database_connection() -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect("postgresql://postgres:postgres@localhost/postgres", NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

async fn drop_table(table_name: &str) -> Result<()>{
    let client = database_connection();

    let query = format!("DROP TABLE {};", table_name);
    client
        .await.execute(&query, &[])
        .await.expect("failed to drop the table");

    println!("{} table dropped successfully", table_name);

    Ok(())
}

enum TableType {
    Users,
    Transactions
}

async fn create_table(table_name: &str, table_type: TableType) -> Result<()>{
    let client = database_connection();

    let create_table_query = match table_type {
        TableType::Users => format!(
            "CREATE TABLE {} (
                userid INT PRIMARY KEY,
                name VARCHAR(100),
                balance FLOAT8
            )",
            table_name
        ),
        TableType::Transactions => format!(
            "CREATE TABLE {} (
                id SERIAL PRIMARY KEY,
                date VARCHAR(100),
                userid INT,
                amount FLOAT8,
                category VARCHAR(100)
            )",
            table_name
        ),
    };
    client.await.execute(&create_table_query, &[]).await.unwrap();

    println!("{} table created", table_name);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    Ok(())
}