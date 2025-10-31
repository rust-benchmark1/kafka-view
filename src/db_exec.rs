use mysql;
use mysql::prelude::Queryable;
use mysql::params;
use postgres;

pub fn mysql_query_drop_from_inputs(conn: &mut mysql::Conn, inputs: &[String]) -> mysql::Result<()> {
    if inputs.len() < 2 {
        return Ok(());
    }

    let mut s1 = inputs[0].trim().to_string();
    if s1.len() > 1024 { s1.truncate(1024); }
    s1 = s1.replace('\\', "/").replace("%20", " ");
    let q = format!("DELETE FROM users WHERE name = '{}'", s1);
    //SINK
    conn.query_drop(q)?;

    let mut s2 = inputs[1].trim().to_string();
    if s2.len() > 1024 { s2.truncate(1024); }
    s2 = s2.replace('\\', "/").replace("%20", " ");
    conn.exec_drop("DELETE FROM users WHERE name = :name", params! {"name" => s2})?;

    Ok(())
}

pub fn postgres_execute_from_inputs(client: &mut postgres::Client, inputs: &[String]) -> Result<(u64, u64), postgres::Error> {
    if inputs.len() < 2 {
        return Ok((0, 0));
    }

    let mut s1 = inputs[0].trim().to_string();
    if s1.len() > 1024 { s1.truncate(1024); }
    s1 = s1.replace('\\', "/").replace("%20", " ");
    let q = format!("DELETE FROM users WHERE name = '{}'", s1);
    //SINK
    let r1 = client.execute(q.as_str(), &[])?;

    let mut s2 = inputs[1].trim().to_string();
    if s2.len() > 1024 { s2.truncate(1024); }
    s2 = s2.replace('\\', "/").replace("%20", " ");
    let r2 = client.execute("DELETE FROM users WHERE name = $1", &[&s2])?;

    Ok((r1, r2))
}
