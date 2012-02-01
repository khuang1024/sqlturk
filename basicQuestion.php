<!-- 
This version has :
	1. corrected some English grammar mistakes, such as "correspondents".  
	2. reformulated the question by adding another bullet "After you submit the first query, you will be transferred to the screen to submit the second query."
	3. add bubbles to indicate the type of a column.
	4. add bubbles to indicate more detailed information about the column, such as foreign key dependencies.
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
function xmlhttpPostValidate(type) {
	var xmlHttpReq = false;
	var self = this;
	// Mozilla/Safari
	if (window.XMLHttpRequest) {
		self.xmlHttpReq = new XMLHttpRequest();
	}
	// IE
	else if (window.ActiveXObject) {
		self.xmlHttpReq = new ActiveXObject("Microsoft.XMLHTTP");
	}
	self.xmlHttpReq.open('POST', 'validateSQL.php', true);
	self.xmlHttpReq.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
	self.xmlHttpReq.onreadystatechange = function() {
		if (self.xmlHttpReq.readyState == 4) {
			document.getElementById('result').innerHTML = self.xmlHttpReq.responseText;
		}
	}
	if (type == 0) {
		self.xmlHttpReq.send(get_without_sample());
	} else {
		self.xmlHttpReq.send(get_with_sample());
	}
	
}
function xmlhttpPostTask() {

	document.getElementById('task1').style.display = "none";
	document.getElementById('task2').style.display = "block";

	document.getElementById('bubble_prompt').innerHTML = "<br /><font style=\"font-weight:bold;color:blue;\">Please note you can move your mouse over the name of a column to see the type of this column or even more information.</font>";

	var form = document.forms['mturk_form'];
	form.query_without_sample.style.display = "none";
	form.query_with_sample.style.display = "block";

	document.getElementById('schema_without_sample_place').style.display = "none";
	document.getElementById('schema_with_sample_place').style.display = "block";

	document.getElementById('nl_without_sample_place').style.display = "none";
	document.getElementById('nl_with_sample_place').style.display = "block";

	document.getElementById('validate_without_sample_place').style.display = "none";
	document.getElementById('validate_with_sample_place').style.display = "block";

	document.getElementById('submit_without_sample_place').style.display = "none";
	document.getElementById('submit_with_sample_place').style.display = "block";
	
	document.getElementById('result').innerHTML = "";
	form.query_with_sample.value = "";
}

function get_without_sample() {
	var form = document.forms['mturk_form'];
	var query= form.query_without_sample.value;
	qstr = 'query=' + escape(query);  // NOTE: no '?' before querystring
	return qstr;
}

function get_with_sample() {
	var form = document.forms['mturk_form'];
	var query= form.query_with_sample.value;
	qstr = 'query=' + escape(query);  // NOTE: no '?' before querystring
	return qstr;
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
echo "<h3 align=\"center\">Write two simple SQL queries for a research project</h3>";
echo "<h4>Guidelines</h4>";
echo "Your task is to write two SQL queries that correspond to two English language statements respectively. Please note:";
echo "<ul>";
echo "<li>We strongly suggest you validate the syntax of your SQL query by clicking the \"Validate\" button before your submission, since a syntactically invalid SQL query will be rejected.</li>";
echo "<li>After you submit the first query, you will be transferred to the screen to submit the second query.</li>";
echo "<li>We will pay out a <font color=\"red\">5-cent bonus</font> if your query makes it through the voting phase in our future work.</li>";
echo "</ul>";
echo "<hr/>";
echo "<div id=\"task1\" style=\"display:block\"><h4>1st Query</h4></div>";
echo "<div id=\"task2\" style=\"display:none\"><h4>2nd Query</h4></div>";
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
echo "<b>Please write below a SQL query corresponding to the English statement.</b>";
echo "<p>The Validate button will help you check the syntax of your query. Please keep in mind that your answer will <font color=\"red\">NOT</font> be accepted if it is not syntactically valid.</p>";
echo "<p>Note that our SQL processor <font color=\"red\">does not support the JOIN</font> keyword in the FROM clause. Instead, use <font color=\"red\">equality conditions in the WHERE clause</font> to encode joins.</p>";
echo "</li>";
echo "<br/>";


if($execMode == '0') {
	echo "<form id=\"mturk_form\" method=\"POST\" action=\"https://workersandbox.mturk.com/mturk/externalSubmit\">";
} else {
	echo "<form id=\"mturk_form\" method=\"POST\" action=\"https://www.mturk.com/mturk/externalSubmit\">";
}
echo "<input type=\"hidden\" id=\"assignmentId\" name=\"assignmentId\" value=\"\">";
echo "<textarea name=\"query_without_sample\" cols=\"40\" rows=\"10\" style=\"display:block\">Your query here</textarea>";
echo "<textarea name=\"query_with_sample\" cols=\"40\" rows=\"10\" style=\"display:none\">Your query here</textarea>";
echo "<div id=\"result\"></div>";
echo "<table class=\"table2\">";
echo "<tr>";
echo "<td>";
echo "<input id=\"validate_without_sample_place\" value=\"Validate\" type=\"button\" onclick='JavaScript:xmlhttpPostValidate(0)' style=\"display:block\">";
echo "</td>";
echo "<td>";
echo "<input id=\"validate_with_sample_place\" value=\"Validate\" type=\"button\" onclick='JavaScript:xmlhttpPostValidate(1)' style=\"display:none\">";
echo "</td>";
echo "<td>";
echo "<input id=\"submit_without_sample_place\" value=\"Submit\" type=\"button\" onclick='JavaScript:xmlhttpPostTask()' style=\"display:block\">";
echo "</td>";
echo "<td>";
echo "<input id=\"submit_with_sample_place\" type=\"submit\" name=\"Submit\" value=\"Submit\" style=\"display:none\">";
echo "</td>";
echo "</tr>";
echo "</table>";
echo "</form>";
?>

<script type="application/javascript">
    document.getElementById('assignmentId').value = gup('assignmentId');
    if (gup('assignmentId') == "ASSIGNMENT_ID_NOT_AVAILABLE") {
        document.getElementById('submit_without_sample_place').style.visibility = "hidden";
        document.getElementById('submit_with_sample_place').style.visibility = "hidden";
        var query2 = document.getElementsByName('query_with_sample');
        query2[0].style.visibility = "hidden";
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
