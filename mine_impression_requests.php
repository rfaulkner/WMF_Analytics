<?php error_reporting (E_NOTICE && E_ALL);

/* 
* mine_impression_requests.php <input_file> <date>
* date is optional and should conform to the following format '2009-11-18'
*/

if ( isset( $argv[2] ) ) {
	$date_passed = date("Y-m-d", strtotime( $argv[2] ) );
} else {
	$date_passed = date("Y-m-d", strtotime("now"));
}


$file_path = '/a/squid/fundraising/logs/';
$file_name = 'landingpages2010.log';

if (!$argv[1]) {
	$file_date = date( 'Ymd', strtotime( 'now' ) );
	$full_path = $file_path . $file_name . $file_date;
	print "Opening $full_path \n";
	$handle = fopen( $full_path, 'r' ) or die ("Could not open file $full_path");
} else {
	if ( preg_match( "/gz/", $argv[1] ) ) { 
		$handle = gzopen($argv[1], 'r' ) or die ("Could not open file $argv[1]");	
	} else {
		$handle = fopen($argv[1], 'r') or die ("Could not open file $argv[1]");
	}	
}

$counts = array();

print "Impressions for $date_passed\n";

while(( $row = fgetcsv( $handle,'',' ') ) !== FALSE) {
 	$datetime = $row[2];
	$date = substr( $datetime, 0, 10 );

	if ( strcmp( $date, $date_passed ) != 0 ) {
		continue;
	}	

	$tracked_url = $row[8];
	$parsed_url = parse_url( $tracked_url );
	parse_str( $parsed_url['query'], $parsed_query );

	if ( $parsed_query['hidden'] == 'false' ) {
		$project = $parsed_query['db'];
		$banner = $parsed_query['utm_source'];
		if ( $banner == 'test' ) {
			continue;
		}
		$lang = $parsed_query['userLang'];

		if ( $project == '' ) {
			// print "Missing DB: $target_url\n";
			$counts[$banner]['NONE'];
		} else {
			$counts[$banner][$project]++;
		}
	}
}

print_r($counts);
?>
