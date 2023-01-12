
use json;


pub fn parse_record(s: &str) -> Result<json::JsonValue, String> {

    return match json::parse(s) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}