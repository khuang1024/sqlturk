<!-- 
Done:
1. Hide some text inputs first, and show them later.
2. Validate the inputs.
 -->

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">

<head>

<!-- meta data -->
<meta http-equiv="content-type" content="text/html;charset=utf-8" />

<!-- css data -->
<link rel="stylesheet" type="text/css" href="http://craigsworks.com/projects/qtip2/packages/latest/jquery.qtip.min.css" />

<!-- jquery and qtip -->
<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js"></script>
<script type="text/javascript" src="http://craigsworks.com/projects/qtip2/packages/latest/jquery.qtip.min.js"></script>

<!-- css data -->
<style type="text/css">
table.table1
{
border-collapse:collapse;
font-family:arial;
font-size:13px
}
table.table1 td, th
{
border:1px solid black;
}
</style>

<script type="application/javascript">

/*
 * This function parses the parameters.
 */
function gup( name ) {
	var regexS = "[\\?&]"+name+"=([^&#]*)";
	var regex = new RegExp( regexS );
	var tmpURL = window.location.href;
	var results = regex.exec( tmpURL );
	if( results == null )
	return "";
	else
	return results[1];
}

/*
 * This function validates if the inputs are valid.
 */
function validateRanking(id) {
    
    var form = document.forms['mturk_form'];
    var queries;
    if (id == 0) {
        queries = form.elements['q1[]'];
    } else {
        queries = form.elements['q2[]'];
    }
    for (var i = 0; i < queries.length; i++) {
        if (queries[i].value > 10 || queries[i].value < 1) {
            alert("Numbers must be beteen 1 and 10! Please check your inputs.");
            return false;
        }
        for (var j = i+1; j < queries.length; j++){
            if (queries[i].value == queries[j].value) {
                alert("Numbers must be different from each other! Please check your inputs.");
                return false;
            }
        } 
    }
    return true;
}

/*
 * This function transfers workers to the second subsection
 * by using Ajax.
 */
function xmlhttpPostTask() {

    if (validateRanking(0)) {
    	document.getElementById('task1').style.display = "none";
    	document.getElementById('task2').style.display = "block";

        document.getElementById('second_ranking').style.display = "block";
        document.getElementById('first_ranking').style.display = "none";
    	document.getElementById('schema_without_sample_place').style.display = "none";
    	document.getElementById('schema_with_sample_place').style.display = "block";
    
    	document.getElementById('nl_without_sample_place').style.display = "none";
    	document.getElementById('nl_with_sample_place').style.display = "block";
    }
}

/*
 * This function handles the submission of the form.
 */
function submitForm() {
    if (validateRanking(1)) {
        document.getElementById('mturk_form').submit();
        //document.forms['mturk_form'].submit();
    } 
}

</script>


</head>

<body style="font-family:arial;font-size:13px">

<script type="application/javascript">
var _gaq = _gaq || [];
_gaq.push(['_setAccount', 'UA-3828080-12']);
_gaq.push(['_trackPageview']);

(function() {
	var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
	ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
	var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
})();
</script>

<?php
// connect to database and retrieve relations and schemas
$link = mysql_pconnect('mysql.cse.ucsc.edu', 'abogdan', 'FJyJqqJs') or die('Database connection error: Please continue without validation');
mysql_select_db('abogdan') or die('Database connection error: Please continue without validation');

// get the relation list from $_GET
$relsList = $_GET["rels"];
$rels = explode(" ",trim($relsList)); // change into an array
$nlQuery1 = $_GET["nlQuery1"];
$nlQuery2 = $_GET["nlQuery2"];
$execMode = $_GET["execMode"];
$q1 = $_GET["q1"];
$q2 = $_GET["q2"];
$q3 = $_GET["q3"];
$q4 = $_GET["q4"];
$q5 = $_GET["q5"];
$q6 = $_GET["q6"];
$q7 = $_GET["q7"];
$q8 = $_GET["q8"];
$q9 = $_GET["q9"];
$q10 = $_GET["q10"];
$q11 = $_GET["q11"];
$q12 = $_GET["q12"];
$q13 = $_GET["q13"];
$q14 = $_GET["q14"];
$q15 = $_GET["q15"];
$q16 = $_GET["q16"];
$q17 = $_GET["q17"];
$q18 = $_GET["q18"];
$q19 = $_GET["q19"];
$q20 = $_GET["q20"];

$schema_without_sample = "";
$schema_with_sample = "";


// initialize $schema_without_sample and $schema_with_sample
foreach($rels as $relName) {
	
	$nColumns = 0;
	$column_toString = "";
	$column_toString_with_bubble = "";
	$table = "";
	
	// evaluate $schema_without_sample
	$result = mysql_query("SHOW COLUMNS FROM $relName");
	if (!$result) {
		$returnedError = 'error=Could not run query: ' . mysql_error();
		exit;
	}
	if (mysql_num_rows($result) > 0) {
		$isFirst = true;
		$index = 0;
		while ($row = mysql_fetch_assoc($result)) {
			$attName = $row["Field"];
			$attType = $row["Type"];
			// skip __ROWID
			if(strcmp($attName,"__ROWID")!=0) {
				if($isFirst===false) {
					$column_toString = $column_toString . ", ";
					$column_toString_with_bubble = $column_toString_with_bubble . ",";
				}
				
				// for comments ********************
				$comment_result = mysql_query("SELECT COMMENT FROM DESCRIPTION WHERE TABLE_NAME='" . $relName . "' AND COLUMN_NAME='".$attName."'");
				if (!$comment_result) {
					$returnedError = 'error=Could not run query: ' . mysql_error();
					exit;
				}
				if (mysql_num_rows($comment_result) > 0) {
					$comment_row = mysql_fetch_assoc($comment_result);
					$attType = $attType . ", " . $comment_row["COMMENT"];
				}
				// for comments ********************
				
				$column_names[$nColumns] = $attName;
				$column_types[$nColumns] = $attType;
								
				$column_toString = $column_toString . $attName;
				$column_toString_with_bubble = $column_toString_with_bubble . "<a title=\"" . $attType . "\">" . $attName . "</a>";
				
				$nColumns = $nColumns + 1;
				$isFirst=false;
			}
		}
	}
	$schema_without_sample = $schema_without_sample . "<p style=\"color:red\">" . $relName . " (" . $column_toString_with_bubble . ")</p>";
	
	// evaluate $schema_with_sample
	$schema_with_sample = $schema_with_sample . "<p style=\"color:red\">" . $relName . " (" . $column_toString_with_bubble . ")</p>";
	$table = "<table class=\"table1\">";
	$header = "<tr>";
	for ($i = 0; $i < $nColumns; $i++) {
		$header = $header . "<td><a title=\"" . $column_types[$i] . "\">" . $column_names[$i] . "</a></td>";
	}
	$header = $header . "</tr>";
	$table = $table . $header;
	
	
	$result = mysql_query("SELECT " . $column_toString . " FROM " . $relName . " LIMIT 5");
	while($row = mysql_fetch_array($result)) {
		$table = $table . "<tr>";
		for ($i = 0; $i < $nColumns; $i++) {
			$table = $table . "<td>" . $row[$i] . "</td>";
		}
		$table = $table . "</tr>";
	}
	$table = $table . "</table>";
	$schema_with_sample = $schema_with_sample . $table;
}

// html code
echo "<h3 align=\"center\">Rank queries based on the need and schema</h3>";
echo "<h4>Guidelines</h4>";
echo "Your task is to rank some queries based on the need which is given as an English statement and the related schema. Please note:";
echo "<ul>";
echo "<li>There are 10 queries in total.</li>";
echo "<li>You rank these queries by putting numbers in the box.</li>";
echo "<li>These number must be <font color=\"red\">different</font> from each other and <font color=\"red\">between 1 and 10</font>. That is, they are 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 respectively.</li>";
echo "<li>A <font color=\"red\">smaller</font> number means a <font color=\"red\">better</font> query. For example, 1 is for the best query in your mind.</li>";
echo "<li>Here are two ranking subsections. After you submit the first one, you will be transferred to the screen to submit the second one.</li>";
echo "</ul>";
echo "<hr/>";
echo "<div id=\"task1\" style=\"display:block\"><h4>1st Ranking</h4></div>";
echo "<div id=\"task2\" style=\"display:none\"><h4>2nd Ranking</h4></div>";
echo "<ul>";
echo "<li>";
echo "<b>Consider the following relations in a database:</b>";
//echo "<div id=\"bubble_prompt\"></div>";
echo "<div id=\"bubble_prompt\"><br /><font style=\"font-weight:bold;color:blue;\">Please note you can move your mouse over the name of a column to see the type of this column or even more information.</font></div>";
echo "<div id=\"schema_without_sample_place\" style=\"display:block\">" . $schema_without_sample . "</div>";
echo "<div id=\"schema_with_sample_place\" style=\"display:none\">" . $schema_with_sample . "</div>";
echo "</li>";
echo "<br/>";
echo "<li>";
echo "<b>Consider the following English statement:</b>";
echo "<p><div id=\"nl_without_sample_place\" style=\"display:block;color:red\">" . $nlQuery1 . "</div></p>";
echo "<p><div id=\"nl_with_sample_place\" style=\"display:none;color:red\">" . $nlQuery2 . "</div></p>";
echo "</li>";
echo "<br/>";
echo "<li>";
echo "<b>Please rank the SQL queries below corresponding to the English statement and the schema.</b>";
echo "</li>";
echo "<br/>";


if($execMode == '0') {
	echo "<form id=\"mturk_form\" method=\"POST\" action=\"https://workersandbox.mturk.com/mturk/externalSubmit\">";
} else {
	echo "<form id=\"mturk_form\" method=\"POST\" action=\"https://www.mturk.com/mturk/externalSubmit\">";
}
echo "<input type=\"hidden\" id=\"assignmentId\" name=\"assignmentId\" value=\"\">";

// the first group of ranking
echo "<div id=\"first_ranking\">";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q1 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q2 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q3 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q4 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q5 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q6 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q7 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q8 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q9 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q1[]\" />" . $q10 . "<br />";
echo "<input id=\"submit_without_sample_place\" value=\"Submit\" type=\"button\" onclick='JavaScript:xmlhttpPostTask()'>";
echo "</div>";

// the second group of ranking
echo "<div id=\"second_ranking\">";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q11 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q12 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q13 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q14 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q15 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q16 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q17 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q18 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q19 . "<br />";
echo "<input type=\"text\" size=\"2\" maxlength=\"2\" name=\"q2[]\" />" . $q20 . "<br />";
//echo "<input id=\"submit_with_sample_place\" type=\"submit\" name=\"Submit\" value=\"Submit\">";
echo "<button id=\"submit_with_sample_place\" type=\"button\" onClick=\"submitForm()\">submit</button>";
echo "</div>";
echo "</form>";
?>

<script type="application/javascript">
    document.getElementById('assignmentId').value = gup('assignmentId');
    document.getElementById('second_ranking').style.display = "none";


    if (gup('assignmentId') == "ASSIGNMENT_ID_NOT_AVAILABLE") {
        document.getElementById('submit_without_sample_place').style.visibility = "hidden";
        document.getElementById('submit_with_sample_place').style.visibility = "hidden";
        document.getElementById('second_ranking').style.display = "none";
    }
</script>


<script type="text/javascript">
$(document).ready(function()
{
	// Match all <A/> links with a title tag and use it as the content (default).
	$('a[title]').qtip();
});
</script>

</body>
</html>
