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

while(( $row = fgetcsv( $handle,'',' ') ) !== FALSE) {

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
			$counts[$banner]['NONE']++;
		} else {
			$counts[$banner][$project]++;
		}
	}
}

$stdout = fopen("php://stdout", "a");
fputcsv($stdout, array ( 
	"banner", "project", "counts",	
	)
);

foreach($counts as $bannername => $project_array){
	foreach ($project_array as $project => $count){
		fputcsv($stdout, array( $bannername, $project, $count));
	}
}

//print_r($counts);
?>
