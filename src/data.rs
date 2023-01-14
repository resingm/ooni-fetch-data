
use serde_json::Value;
use chrono::{DateTime, NaiveDateTime, Utc};
use crypto::{sha1::Sha1, digest::Digest};

// use sha1::{Sha1, Digest, digest::{DynDigest, Update}};
// use uuid::Uuid;

static ANNOTATIONS : &str = "annotations";
static ARCHITECTURE : &str = "architecture";
static ENGINE_NAME : &str = "engine_name";
static ENGINE_VERSION : &str = "engine_version";
static FLAVOR : &str = "flavor";
static INPUT: &str = "input";
static NETWORK_TYPE : &str = "network_type";
static ORIGIN : &str = "origin";
static PLATFORM : &str = "platform";
static PROBE_ASN : &str = "probe_asn";
static PROBE_CC : &str = "probe_cc";
static PROBE_IP : &str = "probe_ip";
static PROBE_NETWORK_NAME : &str = "probe_network_name";
static REPORT_ID: &str = "report_id";
static RESOLVER_ASN : &str = "resolver_asn";
static RESOLVER_IP : &str = "resolver_ip";
static RESOLVER_NETWORK_NAME : &str = "resolver_network_name";
static TEST_RUNTIME : &str = "test_runtime";
static TEST_START_TIME : &str = "test_start_time";
static TEST_VERSION : &str = "test_version";
static TS: &str = "measurement_start_time";


#[derive(Debug)]
pub struct Measurement {
    // Timeseries data & pivoting
    ts: DateTime<Utc>,
    fqdn: String,           // categorical
    report_id: String,
    input: String,
    digest: String,

    test_start_time: DateTime<Utc>,
    test_runtime: f64,
    test_version: String,   // categorical

    // Annotation fields
    architecture: String,   // categorical
    engine_name: String,    // categorical
    engine_version: String, // categorical
    flavor: String,         // categorical
    network_type: String,   // categorical
    origin: String,         // categorical
    platform: String,       // categorical

    // Probe
    probe_asn: String,
    probe_cc: String,       // categorical
    probe_ip: String,
    probe_network_name: String,

    // Resolver
    resolver_asn: String,
    resolver_ip: String,
    resolver_network_name: String,
}

impl Measurement {
    fn new(
        ts: DateTime<Utc>,
        report_id: String,
        input: String,
        test_start_time: DateTime<Utc>,
        test_runtime: f64,
        test_version: String,
        architecture: String,
        engine_name: String,
        engine_version: String,
        flavor: String,
        network_type: String,
        origin: String,
        platform: String,
        probe_asn: String,
        probe_cc: String,
        probe_ip: String,
        probe_network_name: String,
        resolver_asn: String,
        resolver_ip: String,
        resolver_network_name: String,
    ) -> Measurement {
        let fqdn = String::from(input.as_str())
            .replace("http://", "")
            .replace("https://", "");
            // .split_once("/");
        
        let fqdn = match fqdn.split_once("/") { 
            Some((x, _)) => String::from(x),
            None => String::from(fqdn),
        };

        let mut hasher = Sha1::new();
        hasher.input(&report_id.as_bytes());
        hasher.input(&fqdn.as_bytes());
        hasher.input(&ts.to_rfc3339().as_bytes());
        hasher.input(input.as_str().as_bytes());
        let digest = hasher.result_str();

        // let ts = DateTime::parse_from_rfc3339(&ts).unwrap();

        Measurement {
            ts,
            fqdn,
            report_id,
            input,
            digest,
            test_start_time,
            test_runtime,
            test_version,
            architecture,
            engine_name,
            engine_version,
            flavor,
            network_type,
            origin,
            platform,
            probe_asn,
            probe_cc,
            probe_ip,
            probe_network_name,
            resolver_asn,
            resolver_ip,
            resolver_network_name,
        }
    }

    fn from_serde(o: Value) -> Result<Measurement, String> {

        let o = match o.as_object() {
            Some(x) => x,
            None => return Err(String::from("Failed to read JSON as SerDe object.")),
        };

        let ts = match o[TS].as_str() {
            Some(x) => String::from(x),
            None => return Err(format!("Missing attribute '{}'", TS)),
        };

        let report_id = match o[REPORT_ID].as_str() {
            Some(x) => String::from(x),
            None => return Err(format!("Missing attribute '{}'", REPORT_ID)),
        };

        let input = match o[INPUT].as_str() {
            Some(x) => String::from(x),
            None => return Err(format!("Missing attribute '{}'", INPUT)),
        };

        let annotations = match o[ANNOTATIONS].as_object() {
            Some(x) => x,
            None => return Err(format!("Missing node '{}'", ANNOTATIONS)),
        };

        let ts = match NaiveDateTime::parse_from_str(&ts, "%Y-%m-%d %H:%M:%S") {
            Ok(x) => DateTime::<Utc>::from_utc(x, chrono::Utc),
            Err(e) => return Err(format!("Failed to parse timestamp: {}", e.to_string())),
        };

        let test_start_time = match NaiveDateTime::parse_from_str(
            o[TEST_START_TIME].as_str().unwrap_or_default(), 
            "%Y-%m-%d %H:%M:%S"
        ) {
            Ok(x) => DateTime::<Utc>::from_utc(x, chrono::Utc),
            Err(e) => return Err(format!("Failed to parse timestamp: {}", e.to_string())),
        };
        
        Ok(Measurement::new(
            ts,
            report_id,
            input,
            test_start_time,
            o[TEST_RUNTIME].as_f64().unwrap_or_default(),
            o[TEST_VERSION].as_str().unwrap_or_default().to_string(),
            annotations[ARCHITECTURE].as_str().unwrap_or_default().to_string(),
            annotations[ENGINE_NAME].as_str().unwrap_or_default().to_string(),
            annotations[ENGINE_VERSION].as_str().unwrap_or_default().to_string(),
            annotations[FLAVOR].as_str().unwrap_or_default().to_string(),
            annotations[NETWORK_TYPE].as_str().unwrap_or_default().to_string(),
            annotations[ORIGIN].as_str().unwrap_or_default().to_string(),
            annotations[PLATFORM].as_str().unwrap_or_default().to_string(),
            o[PROBE_ASN].as_str().unwrap_or_default().to_string(),
            o[PROBE_CC].as_str().unwrap_or_default().to_string(),
            o[PROBE_IP].as_str().unwrap_or_default().to_string(),
            o[PROBE_NETWORK_NAME].as_str().unwrap_or_default().to_string(),
            o[RESOLVER_ASN].as_str().unwrap_or_default().to_string(),
            o[RESOLVER_IP].as_str().unwrap_or_default().to_string(),
            o[RESOLVER_NETWORK_NAME].as_str().unwrap_or_default().to_string(),
        ))
    }
}


pub fn parse_record(s: &str) -> Result<Measurement, String> {

    let o: Value = match serde_json::from_str(s) {
        Ok(o) => o,
        Err(e) => return Err(e.to_string()),
    };

    Measurement::from_serde(o)
}