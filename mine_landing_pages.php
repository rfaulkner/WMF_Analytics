<?php error_reporting (E_NOTICE && E_ALL);

/* 
 * mine_landing_pages.php <input_file> <date>
 * date is optional and should conform to the following format '2009-11-18'
 */


if ( isset( $argv[2] ) ) {
	$date_passed = $argv[2];
} else {
	$date_passed = date("Y-m-d", strtotime("now"));
}


$file_path = '/a/squid/fundraising/logs/';
$file_name = 'landingpages2010.log';

if( !$argv[1] ) {
	$file_date = date( 'Ymd', strtotime( 'now' ) );
	$full_path = $file_path . $file_name . $file_date;
	print "Opening $full_path \n";
	$handle = fopen( $full_path, 'r' ) or die ("Could not open file $full_path\n");
} else {
	if ( preg_match( "/gz/", $argv[1] ) ) { 
		$handle = gzopen($argv[1], 'r' ) or die ("Could not open file $argv[1]\n");	
	} else {
		$handle = fopen($argv[1], 'r') or die ("Could not open file $argv[1]\n");
	}	
}

$counts = array();
$counts_csv = '';

print "Clicks for $date_passed\n";

while(( $row = fgetcsv($handle,'',' ') ) !== FALSE) {
 	$datetime = $row[2];
	$date = substr( $datetime, 0, 10 );

	if ( strcmp( $date, $date_passed ) != 0 ) {
		continue;
	}	

	$referrer_url = $row[11];
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


	// Get the landing page
	$landing_url = $row[8];
	$parsed_landing_url = parse_url( $landing_url );
	$landing_path = explode( '/' , $parsed_landing_url['path'] );
	$landing_page = $landing_path[2];

	// Get the banner name and lang
	parse_str( $parsed_landing_url['query'], $tracking );
	$utm_source = $tracking['utm_source'];

	if ( preg_match( '/^2010/', $utm_source) && ! preg_match( '/utm_campaign/', $utm_source  ) ) { // Second part removes some noise
		//$counts[$source_lang][$project][$lang][$utm_source][$landing_page]++;
		//$counts[$project][$utm_source][$landing_page]++;
		$counts[$utm_source][$project][$landing_page]++;
	}	
}

print_r($counts);


//Quick hack to get it in

$csv_yes = 1;
if ( $csv_yes ) {	
	//$fp = fopen('file.csv', 'w');
	foreach( $counts as $utm_source => $project ) { 
		foreach( $project as $project_p => $landing_page ) {
			foreach( $landing_page as $landing_page_p => $amount ) { 
				print $utm_source . ',' . $project_p . ',' . $landing_page_p . ',' . $amount . "\n";
			}
		}
	//	fputcsv( $fp, $counts );
	}
	//fclose($fp);
}
?>
