<?php error_reporting (E_NOTICE && E_ALL);

/**
 * Output landing page stats for a given time range to standard out
 *
 * usage:
 * mine_landing_pages.php <starting ts> <ending ts>
 * ts in the format used in contribution_tracking YYYYMMDDHHiiss
 * starting ts is used in a >= statement
 * ending ts is used in a < statement
 */
$start_ts = $argv[1];
$end_ts = $argv[2];

/**
 * DB connection
 */
$db_host = '';
$db_user = '';
$db_pw = '';
$db_db = '';

// connect to mysql, select the db to use
$db = mysql_connect( $db_host, $db_user, $db_pw );
mysql_select_db( $db_db, $db );

// escape our time stamp strings, just in case.
$start_ts = mysql_real_escape_string( $start_ts, $db );
$end_ts = mysql_real_escape_string( $end_ts, $db );

// initialize some vars for keeping track of stuff
$counts = array();
$counts_csv = '';

// query to select utm source and referrer for our time ranges
$query = "SELECT utm_source, referrer, language FROM contribution_tracking WHERE ts >= '$start_ts' && ts < '$end_ts'";
$result = mysql_query( $query, $db );
if ( !$result ) die( 'There was something wrong with the following query: ' . $query . "\n" . mysql_error() . "\n" );

while( $row = mysql_fetch_object( $result )) {

	$referrer_url = $row->referrer;
	$parsed_url = parse_url( $referrer_url );

	if ( ! isset( $parsed_url['host'] ) ) { //no referrer
		$project = 'NONE';
		$source_lang = 'NONE';
	} else {
		$hostname = explode( '.', $parsed_url['host'] );

		if ( count( $hostname ) <= 2 ) {
			$project = $hostname[0]; //wikimediafoundation.org
			$source_lang = $hostname[0];
		} else {
			$project = ( $hostname[1] == 'wikimedia' ) ? $hostname[0] : $hostname[1]; //species.wikimedia vs en.wikinews
			$source_lang = ( $hostname[1] != 'wikimedia' ) ? $hostname[0] : 'en'; //pl.wikipedia vs commons.wikimedia 
		}
	}

  //parse utm_source
  $utm_source = $row->utm_source;
  $utm_source_parts = explode( ".", $utm_source );
  $counts[ $utm_source_parts[0] ][ $row->language ][ $project ][ $utm_source_parts[1] ]++;
}

print_r($counts);

//Quick hack to get it in

$stdout = fopen("php://stdout", "a");

$csv_yes = 1;
if ( $csv_yes ) {	
	foreach( $counts as $utm_source => $language ) { 
    foreach( $language as $language_p => $project ) {
      foreach( $project as $project_p => $landing_page ) {
	  		foreach( $landing_page as $landing_page_p => $amount ) { 
		  		fputcsv( $stdout, array($utm_source,  $language_p, $project_p,  $landing_page_p ,  $amount ));
        }
      }
		}
	}
}
?>
