<?php error_reporting (E_NOTICE && E_ALL);

/*
* mine_impression_requests.php <input_file>
*/

if ( preg_match( "/gz/", $argv[1] ) ) {
	$handle = gzopen($argv[1], 'r' ) or die ("Could not open file $argv[1]");
} else {
	$handle = fopen($argv[1], 'r') or die ("Could not open file $argv[1]");
}

$counts = array();
$countsNOGEO = array();


while(( $row = fgetcsv( $handle,'',' ') ) !== FALSE) {

	$loader_url = $row[8];
	$parsed_url = parse_url( $loader_url );
	parse_str( $parsed_url['query'], $parsed_query );

	$project = $parsed_query['db'];
	$banner = $parsed_query['banner'];

	if ( $banner == 'test' || preg_match( '/google_ads/', $banner) )  {
		continue;
	}
	$lang = $parsed_query['userlang'];
	$country = $parsed_query['country']; // label each record according to country of origin

	// boolean variable which indicates whether the request is non-geo
	isNonGeo = ($lang == 'pt');

	if ( $project == '' ) {
		// Try to lookup a sitename if the client is using v1 of the loader
		$sitename = $parsed_query['sitename'];
		if ( $sitename ) {

			// Processes NON-GEO banner requests
			if (isNonGeo)
				$counts[$banner][$sitename]++;
			else
				$counts[$banner][$country][$sitename]++;

		} else {

			// Processes NON-GEO banner requests
			if(isNonGeo)
				$counts[$banner]['NONE']++;
			else
				$counts[$banner]['NONE'][$country]++;
		}
	} else {

		// Processes NON-GEO banner requests
		if(isNonGeo)
			$counts[$banner][$project]++;
		else
			$counts[$banner][$project][$country]++;
	}
}

$stdout = fopen("php://stdout", "a");
fputcsv($stdout, array (
	"banner", "project", "counts",
	)
);

foreach($counts as $bannername => $project_array){
	foreach ($project_array as $project => $country_array){
		foreach ($country_array as $country => $count){
			fputcsv($stdout, array( $bannername, $project, $country, $count));
		}
	}
}

//print_r($counts);
?>
