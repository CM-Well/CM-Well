var infoton = {};
var pathToFieldsMap = {};
var fieldsToValues = {};
var fieldSearchClicked = false;
var typeWidgetNotBinded = true;
var ajaxCount = 0; 
var arrowImg = '<img class="searchWidgetIcon" style="cursor: pointer !important;" src="/sys/img/arrow_orange.png" />';
var waitImg = '<img class="searchWidgetIcon" style="cursor: wait !important;" src="/sys/img/wait_orange.png" />';
var xImg = '<img class="searchWidgetIcon" style="cursor: default;" src="/sys/img/x_orange.png" />';
var secret1 = 0x0061;
var secret2 = 0x0061;
var secret3 = 0x0061;
var offset = 0;

$(document).ready(function () {
//	console.log('entering $(document).ready')

    hljs.configure({useBR: false});


	window.onpopstate = function(e){
//		console.log('entering onpopstate');
		if(supports_history_api() && e.state && e.state.url){
			window.history.replaceState(e.state.oldstate, null, e.state.url);
			onHashChange();
		}
//		console.log('leaving onpopstate');
	}
	
    fieldSearchClicked = false;

    $("#fieldList").empty();

    document.getElementById("chooseFieldText").innerHTML=arrowImg + '&lt; Choose &gt;';
	document.getElementById("chooseTypeText").innerHTML=arrowImg + '&lt; Format &gt;';

    var $jamal = $('<img src="/sys/img/jamal.jpg"/>').dialog({autoOpen:false, position: "center"});

    $("#valueText").click(function(){
    	openValueList();
    });

    $(document).bind('keypress', function(e){
    	if(e.which == 0x05D0) {
            $jamal.dialog('open');
        }
        
        secret1 = secret2;
        secret2 = secret3;
        secret3 = e.which;
        
        //if user typed "cmw", do the following:
        if(secret1 == 0x0063 && secret2 == 0x006D && secret3 == 0x0077) {
        	openValueList();
        }
    });

    $('#searchInput').keypress(function(e) {
//        console.log('entering $(\'#searchInput\').keypress')
        if (e.which == 13) {
            $('#searchButton').click();
        }
//        console.log('leaving $(\'#searchInput\').keypress')
    });
    
    if(window.location.hash.length>0) {
        onHashChange();
        bindSearchWidget();
//        console.log('leaving if(window.location.hash.length>0) on $(document).ready')
        return;
    }

    var data = document.getElementById("myresults").textContent;
    if (data == null || data.length == 0) {
        data = document.getElementById("myresults").firstChild.data;
    }
    if (data) {
        var infotonExp = 'infoton = ' + data;
        eval(infotonExp);
        renderInfoton();
    } else {
        onHashChange();
    }

    $('#searchButton').click(function() {
//    	console.log('entering $(\'#searchButton\').click')
        var searchText = $('#searchInput').val();
        if (searchText == ""){
//        	console.log('leaving $(\'#searchButton\').click')
            return false;
        }
            
        var chosenField = $("#chooseField").data('field');
        window.open(infoton.system.path + '?op=search&qp=' + chosenField + ':' + encodeURIComponent(searchText));
//        console.log('leaving $(\'#searchButton\').click')
        return false;
    })
	
	bindSearchWidget();
//	console.log('leaving $(document).ready')
});

function openValueList(){
	var chosenField = $("#chooseField").data('field');
	var values = [];
	if(chosenField == '_all'){
		for(var f in fieldsToValues){
			values = _.union(values, fieldsToValues[f]);
		}
	}
	else {
		values = fieldsToValues[chosenField];
	}
	
    $("#valueList").empty();
            
    var bound = Math.min(20,values.length);
    var sortable = values.slice(0,bound);
	sortable.sort();
	sortable.reverse();
   
	while (sortable.length > 0) {
		var v = sortable.pop();
		var text = v;
		if(v.length > 30) {
			text = v.substring(0,26) + '...';
		}
		$('#valueList').append('<li class="valueItem" title="' + v + '" >' + text + '</li>');
	}

	$(".valueItem").click( function () {
		document.getElementById("searchInput").value = $(this).attr('title');
		$( "#chooseValue" ).dialog( "close" );
	});

	$( "#chooseValue" ).dialog( "open" );
}

function bindTypesWidget() {

    $("#typeList").empty();
    $('#typeList').append('<li class="typeListItem" title="json" >json</li>');
    $('#typeList').append('<li class="typeListItem" title="jsonld" >jsonLD</li>');
    $('#typeList').append('<li class="typeListItem" title="n3" >N3</li>');
    $('#typeList').append('<li class="typeListItem" title="ntriples" >NTriples</li>');
    $('#typeList').append('<li class="typeListItem" title="turtle" >Turtle</li>');
    $('#typeList').append('<li class="typeListItem" title="rdfxml" >RDF Xml</li>');
    $('#typeList').append('<li class="typeListItem" title="yaml" >YAML</li>');

    if(typeWidgetNotBinded) {
        typeWidgetNotBinded = false;
        $('#chooseType').click(function () {
            $("#typeListContainer").slideToggle("slow");
        })
    }

    $(".typeListItem").click( function () {
        var t = $(this).attr('title');
        window.open(infoton.system.path + '?format=' + t + '&override-mimetype=text/plain%3Bcharset=UTF-8');
        $("#typeListContainer").slideUp("slow");
    });
}

function bindSearchWidget(){
//	console.log('entering bindSearchWidget')
	$("#chooseField").data('field','_all');
	$('#chooseField').click( function () {
//	console.log('entering $(\'#chooseField\').click')
		if(!fieldSearchClicked) {
			fieldSearchClicked = true;
			document.getElementById("chooseFieldText").innerHTML=waitImg + 'Loading...';
			$("#fieldList").empty();
			var children = infoton.children;
			if (children == null || (pathToFieldsMap[infoton.system.path] && pathToFieldsMap[infoton.system.path] == {})) {
				document.getElementById("chooseFieldText").innerHTML=xImg + 'N/A';
				$("#chooseField").data('field','_all');
			}
			else {
				if(!pathToFieldsMap[infoton.system.path]) {
					pathToFieldsMap[infoton.system.path] = {};
					var bound = Math.min(50,children.length);
					ajaxCount = bound;
					var reqArr = [];
					
					for(var i=0; i < bound; i++){
						
						reqArr.push($.ajax({
								url : children[i].system.path + '?format=json&length=1',
								type : "GET",
								dataType : 'json',
								timeout : 10000,
								async : true,
								cache : false,
								success: function (data) {
//									console.log('entering success: function');
									var fields = data.fields;
									if (fields != null) {
										$.each(fields, function(f, v) {
											if(!pathToFieldsMap[infoton.system.path][f]) {
										        pathToFieldsMap[infoton.system.path][f] = f;
										        if(!fieldsToValues[f]){
										        	fieldsToValues[f] = [];
										        }
										        fieldsToValues[f] = _.union(fieldsToValues[f],v);
										    }
										});
									}
//									console.log('leaving success: function')
								},
								complete: function ( jqXHR, textStatus ){
//									console.log('entering complete: function');
									ajaxCount--;
									if(ajaxCount == 0){
										fillSearchWidget();
									}
//									console.log('leaving complete: function')
								}
							})
						);
					}//end of: for loop
				}//end of: if(!pathToFieldsMap[infoton.system.path])
				else {
					fillSearchWidget();
				}
			}//end of: else(children == null)
		} //end of: if(!fieldSearchClicked)
		else {
			if(!$.isEmptyObject(pathToFieldsMap[infoton.system.path])) {
				$("#fieldListContainer").slideToggle("slow");
			}
		}
//		console.log('leaving $(\'#chooseField\').click')
	});
//	console.log('leaving bindSearchWidget')
}

function fillSearchWidget() {
//	console.log('entering fillSearchWidget');
	
	if($.isEmptyObject(pathToFieldsMap[infoton.system.path])) {
		document.getElementById("chooseFieldText").innerHTML=xImg + 'N/A';
		$("#chooseField").data('field','_all');
//		console.log('leaving fillSearchWidget')
		return;
	}
	
	var sortable = [];
	for (var field in pathToFieldsMap[infoton.system.path]) {
		sortable.push(field);
	}

	sortable.sort();
	sortable.reverse();
	while (sortable.length > 0) {
		var f = sortable.pop();
		var text = f;
		if(f.length > 30) {
			text = f.substring(0,26) + '...';
		}
		$('#fieldList').append('<li class="listItem" title="' + f + '" >' + text + '</li>');
	}
	
	$(".listItem").click( function () {
//		console.log('entering $(".listItem").click')
		var f = $(this).attr('title');
		var text = f;
		

		if(f.length > 13) {
			text = f.substring(0,11) + '...';
		} else if (f.length < 13) {
			var spaces = Array(13 - f.length).join(" ");
			text = f + spaces;
		} else {
			text = f;
		}
		

		//TODO: beware of special HTML delimeters in the text...!!
		document.getElementById("chooseFieldText").innerHTML=arrowImg + text;
		$("#chooseField").data('field',f);
		$("#fieldListContainer").slideUp("slow");
		
//		console.log('leaving $(".listItem").click')
	});
	
	document.getElementById("chooseFieldText").innerHTML=arrowImg + '&lt; Choose &gt;';
	$("#chooseField").data('field','_all');
	$("#fieldListContainer").slideToggle("slow");
	
//	console.log('leaving fillSearchWidget')
}

function initiateSearchWidget() {
//	console.log('entering initiateSearchWidget')
	fieldSearchClicked = false;
	$("#fieldListContainer").slideUp("fast");
	$("#fieldList").empty();
	document.getElementById("chooseFieldText").innerHTML=arrowImg + '&lt; Choose &gt;';
	$("#chooseField").data('field','_all');
//	console.log('leaving initiateSearchWidget')
}

function initiateTypeListWidget() {
    $("#typeListContainer").slideUp("fast");
    bindTypesWidget();
    document.getElementById("chooseTypeText").innerHTML=arrowImg + '&lt; Format &gt;';
}

//$(document).ajaxError(function(e, xhr, settings, exception) {
//    alert('error in: ' + settings.url + ' \n'+'error:\n' + xhr.responseText );
//});

$(function() {
	$( "#chooseValue" ).dialog({
		autoOpen: false,
		show: {
			effect: "blind",
			duration: 1000
		},
		hide: {
			effect: "explode",
			duration: 1000
		}
	});
});

$(function() {
//    console.log('entering $(function {')
    $(window).hashchange(function() {
//        console.log('entering $(window).hashchange');
        onHashChange();
//        console.log('leaving $(window).hashchange')
    });
//    console.log('leaving $(function {')
});

function onHashChange() {
//	console.log('entering onHashChange');
    initiateSearchWidget();
    initiateTypeListWidget();
	offset = 0;
    var startHere = window.location.hash;
    if (startHere.length == 0) {
        startHere = '/';
    } else {
        startHere = startHere.substr(1);
    }
    getInfoton(startHere + '?format=json&length=12&bend-back');
//    console.log('leaving onHashChange')
}

$(document).ajaxStart(function() {
//    console.log('entering ajaxStart')
    $('*').css('cursor', 'wait');
//    console.log('leaving ajaxStart')
});

$(document).ajaxComplete(function() {
//    console.log('entering ajaxComplete');
    $('*').css('cursor', 'default');
//    console.log('leaving ajaxComplete');
});

function goTo(infotonPath) {
//    console.log('entering goTo');
    if(infotonPath == infoton.system.path) {
        getInfoton(infotonPath + '?format=json&length=12&bend-back');
    } else {
        window.location.hash = infotonPath;
    }
//    console.log('leaving goTo')
}

function getInfoton(surl) {
//	console.log('entering getInfoton')
    $.ajax({
        url: surl,
        dataType: "jsonp",
        jsonp : "callback",
        jsonpCallback: "gotInfoton",
        timeout: 10000,
        error: function e(jqXHR, textStatus, errorThrown) {onAjaxError(jqXHR, textStatus, errorThrown);}
    }
    );
//    console.log('leaving getInfoton')
}

function onAjaxError(jqXHR, textStatus, errorThrown) {
//	console.log('entering onAjaxError')
    log.error('error occured while getting infoton from cmwell service. Error message: ' + textStatus);
    log.error(jqXHR);
    log.error(errorThrown);
    var t, msg;
    var s = true;
    if(textStatus == "timeout") {
        t = 'warning';
        msg = 'Connection with the cmwell service has timeout, please try again.';
        s = false;
    } else {
        t = 'error';
        msg = 'An error occured while getting infoton from cmwell service.';
    }
    $.n(msg, {
            type: t,
            timeout: 5000,
            stick: s,
            fadeSpeed : 1000,
            close : "close",
            effect : "slide"}
    )
//    console.log('leaving onAjaxError')
}

function gotInfoton(info) {
//	console.log('entering gotInfoton')
    infoton = info;
    renderInfoton();
//    console.log('leaving gotInfoton')
}

function leftPagination() {
    offset = Math.max(0, offset - 12);
    getInfoton(infoton.system.path + '?format=json&length=12&bend-back&offset=' + offset);
}

function rightPagination() {
    offset = Math.min(infoton.total - 12, offset + 12);
    getInfoton(infoton.system.path + '?format=json&length=12&bend-back&offset=' + offset);
}

function renderInfoton() {
//	console.log('entering renderInfoton')
	
	//start an ajax callback for the data
    initiateSearchWidget();
    initiateTypeListWidget();
        
    // Render bread crumbs
    $('#breadCrumb').empty();
    // handle root
    $('#breadCrumb').append('<ul id="breadUL"><li><a href="' + "javascript:goTo('/');" + '"></a></li></ul>');
    if (infoton.system.path != '/') {
        var splittedPath = infoton.system.path.split('/');
        splittedPath.splice(0, 1);
        var breadPath = "";
        for (t in splittedPath) {
            var li = document.createElement("li");
            var a = document.createElement("a");
            breadPath = breadPath + '/' + splittedPath[t];
            a.href = 'javascript:goTo("' + encodeURI(breadPath) + '");';
            a.appendChild(document.createTextNode(splittedPath[t]));
            li.appendChild(a);
            $("#breadUL").append(li);
        }
    }

    $('#breadCrumb').jBreadCrumb();

    // render contentRight
    $('.contentRight').empty();
    if (infoton.system.path != '/') {
        $('.contentRight').append('<h1 style="position: relative;top: 0;left: 0;width: 100%">System</h1><table id="sysTable" style="width:100%;position: relative;"/>');
        $('#sysTable').append('<tr><td>path</td><td>' + infoton.system.path + '</td></tr>');
        $('#sysTable').append('<tr><td>modified date</td><td>' + infoton.system.lastModified + '</td></tr>');
        $('#sysTable').append('<tr><td>type</td><td>' + infoton.type + '</td></tr>');
        $('#sysTable').append('<tr><td>uuid</td><td>' + infoton.system.uuid + '</td></tr>');
    } else {
        $('.contentRight').append('<img id="imageHome" src="/sys/img/image_home.png" width="422"/>');
    }

    // Render obj data, if exist
    var fields = infoton.fields;
    var bb = infoton["bend-back"]
    if (!jQuery.isEmptyObject(fields)) {
        $('.contentRight').append('<h1 style="position: relative;">Object</h1><table id="objTable" style="width:100%;position: relative;bottom: 0;"/>');
        for (key in fields) {
            if(! fields.hasOwnProperty(key)) {
                continue;
            }
            // create <tr> element
            var tr = document.createElement("tr");
            // create <td> element for the name
            var tdname = document.createElement("td");
            // add the data from the obj
            tdname.appendChild(document.createTextNode(key));
            // add it to the table row
            tr.appendChild(tdname);
            // Handle the value
            var tdvalue = document.createElement("td");
            for (j in fields[key]) {
                var value = fields[key][j].toString();
                // if it is a link
                var link = isLink(value);
                if (link != null) {
                    var a = document.createElement("a");
                    if(bb && bb[link]){
                        a.href = bb[link];
                        a.appendChild(document.createTextNode(value));
                        tdvalue.appendChild(a);
                        var bbImg = document.createElement("img");
                        var src = document.createAttribute("src");
                        var style = document.createAttribute("style");
                        style.value = "padding-left: 4px";
                        src.value = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48c3ZnIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgd2lkdGg9IjEwIiBoZWlnaHQ9IjEwIj48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtODI2LjQyOSAtNjk4Ljc5MSkiPjxyZWN0IHdpZHRoPSI1Ljk4MiIgaGVpZ2h0PSI1Ljk4MiIgeD0iODI2LjkyOSIgeT0iNzAyLjMwOSIgZmlsbD0iI2ZmZiIgc3Ryb2tlPSIjZmZiMDFjIi8+PGc+PHBhdGggZD0iTTgzMS4xOTQgNjk4Ljc5MWg1LjIzNHY1LjM5MWwtMS41NzEgMS41NDUtMS4zMS0xLjMxLTIuNzI1IDIuNzI1LTIuNjg5LTIuNjg5IDIuODA4LTIuODA4LTEuMzExLTEuMzExeiIgZmlsbD0iI2ZkY2IwMSIvPjxwYXRoIGQ9Ik04MzUuNDI0IDY5OS43OTVsLjAyMiA0Ljg4NS0xLjgxNy0xLjgxNy0yLjg4MSAyLjg4MS0xLjIyOC0xLjIyOCAyLjg4MS0yLjg4MS0xLjg1MS0xLjg1MXoiIGZpbGw9IiNmZmYiLz48L2c+PC9nPjwvc3ZnPg==" //"/sys/img/bblink.gif"
                        bbImg.attributes.setNamedItem(src);
                        bbImg.attributes.setNamedItem(style);
                        var cmwbb = document.createElement("a");
                        cmwbb.href = link;
                        cmwbb.appendChild(bbImg);
                        tdvalue.appendChild(cmwbb);
                    } else {
                        a.href = link;
                        a.appendChild(document.createTextNode(link));
                        tdvalue.appendChild(a);
                    }
                } else {
                    tdvalue.appendChild(document.createTextNode(value));
                }
                if (j < fields[key].length - 1) {
                    tdvalue.appendChild(document.createTextNode(", "));
                }
            }
            tdvalue.appendChild(document.createElement("nobr"))
            // add to row
            tr.appendChild(tdvalue);
            // add the table row to the end of the table
            $('#objTable').append(tr);
        }

    }

    // handle directories part, if exists
    $('#directories').empty();
    $("#pagination").empty();
    var children = infoton.children;
    if (children && infoton.total && infoton.total > 12) {
        var left = document.createElement("a");
        var right = document.createElement("a");
        left.href = 'javascript:leftPagination();';
        right.href = 'javascript:rightPagination();';
        var limg = document.createElement("img");
        var rimg = document.createElement("img");
        var lsrc = document.createAttribute("src");
        var rsrc = document.createAttribute("src");
        lsrc.value = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48IURPQ1RZUEUgc3ZnIFBVQkxJQyAiLS8vVzNDLy9EVEQgU1ZHIDEuMS8vRU4iICJodHRwOi8vd3d3LnczLm9yZy9HcmFwaGljcy9TVkcvMS4xL0RURC9zdmcxMS5kdGQiPjxzdmcgdmVyc2lvbj0iMS4xIiBpZD0iSXNvbGF0aW9uX01vZGUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IiB3aWR0aD0iMTBweCIgaGVpZ2h0PSIxN3B4IiB2aWV3Qm94PSIwIDAgMTAgMTciIGVuYWJsZS1iYWNrZ3JvdW5kPSJuZXcgMCAwIDEwIDE3IiB4bWw6c3BhY2U9InByZXNlcnZlIj48cG9seWdvbiBmaWxsPSIjZmZiMDFjIiBwb2ludHM9IjAsOC41IDgsMCAxMCwyIDQsOC41IDEwLDE1IDgsMTciLz48L3N2Zz4K" ;
        rsrc.value = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48IURPQ1RZUEUgc3ZnIFBVQkxJQyAiLS8vVzNDLy9EVEQgU1ZHIDEuMS8vRU4iICJodHRwOi8vd3d3LnczLm9yZy9HcmFwaGljcy9TVkcvMS4xL0RURC9zdmcxMS5kdGQiPjxzdmcgdmVyc2lvbj0iMS4xIiBpZD0iSXNvbGF0aW9uX01vZGUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IiB3aWR0aD0iMTBweCIgaGVpZ2h0PSIxN3B4IiB2aWV3Qm94PSIwIDAgMTAgMTciIGVuYWJsZS1iYWNrZ3JvdW5kPSJuZXcgMCAwIDEwIDE3IiB4bWw6c3BhY2U9InByZXNlcnZlIj48cG9seWdvbiBmaWxsPSIjZmZiMDFjIiBwb2ludHM9IjEwLDguNSAyLDAgMCwyIDYsOC41IDAsMTUgMiwxNyIvPjwvc3ZnPgo=" ;
        limg.attributes.setNamedItem(lsrc);
        rimg.attributes.setNamedItem(rsrc);
        left.appendChild(limg);
        right.appendChild(rimg);
        left.style.align = "left";
        right.style.align = "right";
        left.style.border = "none";
        right.style.border = "none";
        $('#pagination').append(left);
        $('<span style="padding-left:200px;padding-right:200px;border:none"></span>').appendTo('#pagination'); //space between arrows
        $('#pagination').append(right);
    }

    // first add parent
    var parentA = document.createElement("a");
    parentA.href = "javascript:goTo('" + getParent() + "');";
    parentA.className = 'flushLeft';
    parentA.appendChild(document.createTextNode(".."));
    parentA.style = "border-bottom: 1px dotted #ccc !important; width: 100% !important" ;
    $('#directories').append(parentA);
    $('#directoriesHeader').empty();
    if (children && children.length > 0) {
        $('#directoriesHeader').text("Directory (" + (infoton.offset + 1) + " - " + (infoton.offset + infoton.length) + " out of " +
                infoton.total + " total)");
    } else {
        $('#directoriesHeader').text("Directory");
    }
    if(children){
        for (j in children.slice(0,12)) {
            var child = children[j];
            var path = child.system.path

            var linkCont =  $('<div class="linksCont"></div>');
            var historyOrRss = document.createElement("a");
            historyOrRss.className = "flushRight" ;
            var himg = document.createElement("img");
            var hsrc = document.createAttribute("src");
            var hstyle = document.createAttribute("style");
            var htitle = document.createAttribute("title");
            hsrc.value = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48IURPQ1RZUEUgc3ZnIFBVQkxJQyAiLS8vVzNDLy9EVEQgU1ZHIDEuMC8vRU4iICJodHRwOi8vd3d3LnczLm9yZy9UUi8yMDAxL1JFQy1TVkctMjAwMTA5MDQvRFREL3N2ZzEwLmR0ZCI+PHN2ZyB2ZXJzaW9uPSIxLjAiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IiB3aWR0aD0iMTBweCIgaGVpZ2h0PSIxMHB4IiB2aWV3Qm94PSIwIDAgMTAgMTAiIHhtbDpzcGFjZT0icHJlc2VydmUiPjxnIGlkPSJoaXN0b3J5Ij48ZyBkaXNwbGF5PSJpbmxpbmUiPjxwb2x5Z29uIGZpbGw9IiNmZjgwMDAiIHBvaW50cz0iMCwwIDYsMCA2LDYgMCw2Ii8+PHBvbHlnb24gZmlsbD0iI2ZmZiIgcG9pbnRzPSIxLDEgMiwxIDIsMi41IDQsMi41IDQsMSA1LDEgNSw1IDQsNSA0LDMuNSAyLDMuNSAyLDUgMSw1Ii8+PHBvbHlnb24gZmlsbD0iI2ZmYjAxYyIgcG9pbnRzPSI3LDIuNSA4LDIuNSA4LDggMiw4IDIsNyA3LDciLz48cG9seWdvbiBmaWxsPSIjZmZlMDM4IiBwb2ludHM9IjksNSAxMCw1IDEwLDEwIDQsMTAgNCw5IDksOSIvPjwvZz48L2c+PC9zdmc+DQo=" ;
            hstyle.value = "padding-left: 4px" ;
            htitle.value = "History";
            himg.attributes.setNamedItem(hsrc) ;
            himg.attributes.setNamedItem(hstyle);
            historyOrRss.attributes.setNamedItem(htitle);
            historyOrRss.href = path + "?with-history" ;
            historyOrRss.appendChild(himg);

            if (child.type == "FileInfoton") {
                if(isText(child.content.mimeType)){

                    var fileOrGear = null
                    if(child.system.path.indexOf('/proc/') == 0){
                        fileOrGear = "active" ;
                    } else {
                        fileOrGear = child.content.mimeType ;
                    }
                                                                              //width set to 88% to prevent line breaking when there are more than 1 image displayed in the right section
                    var $a = $("<a  class='flushLeft' mime='" + fileOrGear + "' style='width: 88%' href='#' id='a_" + j + "'>" + path.substring(path.lastIndexOf('/') + 1) + "</a>");
    //                var $tooltipDiv = $('<div class="tooltip"><a href="' + path + '">view</a></div>');
                    var $div = $('<div id="a_' + j + '_div" style="text-align: left; height: 200px; width: 420px; display: none; border: 1px solid #c7c7c7; overflow-y:auto; float: left;">');
                    var $pre = $('<pre id="a_' + j + '_pre" />');
                    $pre.attr('infotonPath', encodeURI(path) + '?format=json');
                    $div.append($pre);
                    $a.click(function (event) {
//                    	console.log('entering 1st $a.click');
                        var preObj = $('#' + event.currentTarget.id + '_pre');
                        var divObj = $('#' + event.currentTarget.id + '_div');
                        if (preObj.text().length != 0) {
                            divObj.slideToggle(50);
//                            console.log('leaving 1st $a.click');
                            return false;
                        } else {
                            window.func1 = function(info) {
//                            	console.log('entering window.func1');
                                if(isMarkdown(info.content.mimeType)){
                                    //var converter = new Showdown.converter();
                                    var converter = new Markdown.Converter();
                                    Markdown.Extra.init(converter);
                                    var formatted = converter.makeHtml(info.content.data);
                                    preObj.html(formatted);
                                    preObj.find('code').each(function(i, block) {
                                        hljs.highlightBlock(block);
                                    });
                                }
                                else {
                                    preObj.text(info.content.data);
                                }
                                divObj.slideToggle(50);
//                                console.log('leaving window.func1');
                            };
                            $.ajax({
                                        url: preObj.attr('infotonPath'),
                                        dataType: "jsonp",
                                        jsonp : "callback",
                                        jsonpCallback: "func1"
                                    });
//                            console.log('leaving 1st $a.click');
                            return false;
                        }
//                        console.log('leaving 1st $a.click')
                    });
                    var plainText = document.createElement("a");
                    plainText.className = "flushRight" ;
                    var img = document.createElement("img");
                    var src = document.createAttribute("src");
                    var style = document.createAttribute("style");
                    var title = document.createAttribute("title");
                    src.value = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAQAAAAnOwc2AAAATklEQVR4XmXKQQqAMAwF0X/7XsSz9CSKiyIIFRFcjMmihmiGbB5fqDCaQJ4j46pzYLUWe+cXZ6OVBhTS0srY2Njp/+XxXXZOLu6EUWAOPX1vnwTBe6qQAAAAAElFTkSuQmCC" ; // "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48c3ZnIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgd2lkdGg9IjEwIiBoZWlnaHQ9IjEwIj48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtODI2LjQyOSAtNjk4Ljc5MSkiPjxyZWN0IHdpZHRoPSI1Ljk4MiIgaGVpZ2h0PSI1Ljk4MiIgeD0iODI2LjkyOSIgeT0iNzAyLjMwOSIgZmlsbD0iI2ZmZiIgc3Ryb2tlPSIjZmZiMDFjIi8+PGc+PHBhdGggZD0iTTgzMS4xOTQgNjk4Ljc5MWg1LjIzNHY1LjM5MWwtMS41NzEgMS41NDUtMS4zMS0xLjMxLTIuNzI1IDIuNzI1LTIuNjg5LTIuNjg5IDIuODA4LTIuODA4LTEuMzExLTEuMzExeiIgZmlsbD0iI2ZkY2IwMSIvPjxwYXRoIGQ9Ik04MzUuNDI0IDY5OS43OTVsLjAyMiA0Ljg4NS0xLjgxNy0xLjgxNy0yLjg4MSAyLjg4MS0xLjIyOC0xLjIyOCAyLjg4MS0yLjg4MS0xLjg1MS0xLjg1MXoiIGZpbGw9IiNmZmYiLz48L2c+PC9nPjwvc3ZnPg==";
                    style.value = "padding-left: 4px";
                    title.value = "Plain Text";
                    img.attributes.setNamedItem(src);
                    img.attributes.setNamedItem(style);
                    plainText.attributes.setNamedItem(title);
                    plainText.href = path + "?override-mimetype=text/plain%3Bcharset=utf-8";
                    plainText.appendChild(img);

                    $(linkCont).append($a);
                    $(linkCont).append(plainText);
                    if(fileOrGear != "active") {
                        $(linkCont).append(historyOrRss);
                    }
                    if(isMarkdown(child.content.mimeType)){
                        var md = document.createElement("a");
                        md.className = "flushRight" ;
                        var mdimg = document.createElement("img");
                        var mdsrc = document.createAttribute("src");
                        var mdstyle = document.createAttribute("style");
                        var mdtitle = document.createAttribute("title");
                        mdsrc.value = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48IURPQ1RZUEUgc3ZnIFBVQkxJQyAiLS8vVzNDLy9EVEQgU1ZHIDEuMS8vRU4iICJodHRwOi8vd3d3LnczLm9yZy9HcmFwaGljcy9TVkcvMS4xL0RURC9zdmcxMS5kdGQiPjxzdmcgdmVyc2lvbj0iMS4xIiBpZD0iSXNvbGF0aW9uX01vZGUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IiB3aWR0aD0iMTBweCIgaGVpZ2h0PSIxMHB4IiB2aWV3Qm94PSIwIDAgMTAgMTAiIHhtbDpzcGFjZT0icHJlc2VydmUiPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iMi41LDcgMy43NSw3IDQuNzUsMCAzLjUsMCIvPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iNS4yNSw3IDYuNSw3IDcuNSwwIDYuMjUsMCIvPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iMSwxLjUgMSwyLjc1IDksMi43NSA5LDEuNSIvPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iMSw0LjI1IDEsNS41IDksNS41IDksNC4yNSIvPjxwb2x5Z29uIGZpbGw9IiNmZmMwMDAiIHBvaW50cz0iNSwxMCAxLDcuNSAxLjI1LDYuNSA1LDguNzUgOC43NSw2LjUgOSw3LjUiLz48L3N2Zz4K" //"data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48IURPQ1RZUEUgc3ZnIFBVQkxJQyAiLS8vVzNDLy9EVEQgU1ZHIDEuMS8vRU4iICJodHRwOi8vd3d3LnczLm9yZy9HcmFwaGljcy9TVkcvMS4xL0RURC9zdmcxMS5kdGQiPjxzdmcgdmVyc2lvbj0iMS4xIiBpZD0iSXNvbGF0aW9uX01vZGUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IiB3aWR0aD0iMTBweCIgaGVpZ2h0PSIxMHB4IiB2aWV3Qm94PSIwIDAgMTAgMTAiIHhtbDpzcGFjZT0icHJlc2VydmUiPjxwb2x5Z29uIGZpbGw9IiNmZmMwMDAiIHBvaW50cz0iMC41LDAgMC41LDEgOS41LDEgOS41LDAiLz48cGF0aCBkPSJNIDEsMS41IEMgMSw3LjUgMSw3LjUgNSw5LjUgQyA5LDcuNSA5LDcuNSA5LDEuNSBaIiBzdHlsZT0ic3Ryb2tlOiNmZmMwMDA7c3Ryb2tlLXdpZHRoOjEiIHN0cm9rZS1saW5lam9pbj0ibWl0ZXIiIGZpbGw9Im5vbmUiLz48cG9seWdvbiBmaWxsPSIjMTM3MWFhIiBwb2ludHM9IjMsNy41IDQsNy41IDUsMi41IDQsMi41Ii8+PHBvbHlnb24gZmlsbD0iIzEzNzFhYSIgcG9pbnRzPSI1LDcuNSA2LDcuNSA3LDIuNSA2LDIuNSIvPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iMi41LDMuNSAyLjUsNC41IDcuNSw0LjUgNy41LDMuNSIvPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iMi41LDUuNSAyLjUsNi41IDcuNSw2LjUgNy41LDUuNSIvPjwvc3ZnPgo=" ; //PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48IURPQ1RZUEUgc3ZnIFBVQkxJQyAiLS8vVzNDLy9EVEQgU1ZHIDEuMS8vRU4iICJodHRwOi8vd3d3LnczLm9yZy9HcmFwaGljcy9TVkcvMS4xL0RURC9zdmcxMS5kdGQiPjxzdmcgdmVyc2lvbj0iMS4xIiBpZD0iSXNvbGF0aW9uX01vZGUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IiB3aWR0aD0iMTBweCIgaGVpZ2h0PSIxMHB4IiB2aWV3Qm94PSIwIDAgMTAgMTAiIHhtbDpzcGFjZT0icHJlc2VydmUiPjxwb2x5Z29uIGZpbGw9IiNmZjgwMDAiIHBvaW50cz0iMC41LDAgMC41LDEgOS41LDEgOS41LDAiLz48cGF0aCBkPSJNIDEsMS41IEMgMSw3LjUgMSw3LjUgNSw5LjUgQyA5LDcuNSA5LDcuNSA5LDEuNSBaIiBzdHlsZT0ic3Ryb2tlOiNmZmMwMDA7c3Ryb2tlLXdpZHRoOjEiIHN0cm9rZS1saW5lam9pbj0ibWl0ZXIiIGZpbGw9Im5vbmUiLz48cG9seWdvbiBmaWxsPSIjMTM3MWFhIiBwb2ludHM9IjMsNy41IDQsNy41IDUsMi41IDQsMi41Ii8+PHBvbHlnb24gZmlsbD0iIzEzNzFhYSIgcG9pbnRzPSI1LDcuNSA2LDcuNSA3LDIuNSA2LDIuNSIvPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iMi41LDMuNSAyLjUsNC41IDcuNSw0LjUgNy41LDMuNSIvPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iMi41LDUuNSAyLjUsNi41IDcuNSw2LjUgNy41LDUuNSIvPjwvc3ZnPgo=";//PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48IURPQ1RZUEUgc3ZnIFBVQkxJQyAiLS8vVzNDLy9EVEQgU1ZHIDEuMS8vRU4iICJodHRwOi8vd3d3LnczLm9yZy9HcmFwaGljcy9TVkcvMS4xL0RURC9zdmcxMS5kdGQiPjxzdmcgdmVyc2lvbj0iMS4xIiBpZD0iSXNvbGF0aW9uX01vZGUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IiB3aWR0aD0iMTBweCIgaGVpZ2h0PSIxMHB4IiB2aWV3Qm94PSIwIDAgMTAgMTAiIHhtbDpzcGFjZT0icHJlc2VydmUiPjxwb2x5Z29uIGZpbGw9IiNmZjgwMDAiIHBvaW50cz0iMSwwIDEsMiA5LDIgOSwwIi8+PHBvbHlnb24gZmlsbD0iI2ZmY2MwMCIgcG9pbnRzPSIxLDIgMSw4IDUsMTAgOSw4IDksMiIvPjxwb2x5Z29uIGZpbGw9IiMxMzcxYWEiIHBvaW50cz0iMyw4IDQsOCA1LDMgNCwzIi8+PHBvbHlnb24gZmlsbD0iIzEzNzFhYSIgcG9pbnRzPSI1LDggNiw4IDcsMyA2LDMiLz48cG9seWdvbiBmaWxsPSIjMTM3MWFhIiBwb2ludHM9IjIuNSw0IDIuNSw1IDcuNSw1IDcuNSw0Ii8+PHBvbHlnb24gZmlsbD0iIzEzNzFhYSIgcG9pbnRzPSIyLjUsNiAyLjUsNyA3LjUsNyA3LjUsNiIvPjwvc3ZnPgo=
                        mdstyle.value = "padding-left: 4px";
                        mdtitle.value = "Markdown";
                        mdimg.attributes.setNamedItem(mdsrc);
                        mdimg.attributes.setNamedItem(mdstyle);
                        md.attributes.setNamedItem(mdtitle);
                        md.href = path + "?pretty";
                        md.appendChild(mdimg);
                        $(linkCont).append(md);
                    }
                    $(linkCont).append($div);


    //                $('#directories').append($tooltipDiv);
    //                $a.tooltip({ effect: 'slide', position: 'center left'});

//                } else if(isImage(child.content.mimeType)) {
//                    var $a = $("<a mime='image' href='#' id='a_" + j + "'>" + path.substring(path.lastIndexOf('/') + 1) + "</a>");
                } else {
                    var mime = 'unknownToCmwell' ;
                    if(isMimeknownToCmwell(child.content.mimeType)) {
                        mime = child.content.mimeType;
                    }
                    var $a = $("<a target='_blank' class='flushLeft' mime='" + mime + "' href='"+ path + "'>" + path.substring(path.lastIndexOf('/') + 1) + "</a>");
                    $(linkCont).append($a);
                    $(linkCont).append(historyOrRss);
                }
            } else if(child.type == "LinkInfoton") {
                var $a = $("<a class='flushLeft' mime='link' href='#' id='a_" + j + "'>" + path.substring(path.lastIndexOf('/') + 1) + "</a>");
//                var $tooltipDiv = $('<div class="tooltip"><a href="' + path + '">view</a></div>');
                var $div = $('<div id="a_' + j + '_div" style="text-align: left; height: 200px; width: 420px; display: none; border: 1px solid #c7c7c7; overflow-y:auto;">');
                var $pre = $('<pre id="a_' + j + '_pre" />');
                $pre.attr('infotonPath', encodeURI(path) + '?format=json');
                $div.append($pre);
                $a.click(function (event) {
//                	console.log('entering 2nd $a.click');
                    var preObj = $('#' + event.currentTarget.id + '_pre');
                    var divObj = $('#' + event.currentTarget.id + '_div');
                    if (preObj.text().length != 0) {
                        divObj.slideToggle(50);
//                        console.log('leaving 2nd $a.click');
                        return false;
                    } else {
                        window.func1 = function(info) {
//                        	console.log('entering  window.func1');
                            if(info.type == "FileInfoton") {
                                if(isText(info.content.mimeType)){
                                    preObj.text(info.content.data);
                                    divObj.slideToggle(50);
                                } else {
//                                    alert(info.system.path.substring(path.lastIndexOf('/') + 1));
                                    window.location = info.system.path;
                                }
                            }else {
                                goTo(encodeURI(info.path));
                            }
//                            console.log('leaving  window.func1')
                        };
                        $.ajax({
                                    url: preObj.attr('infotonPath'),
                                    dataType: "jsonp",
                                    jsonp : "callback",
                                    jsonpCallback: "func1"
                                });
//                        console.log('leaving 2nd $a.click');
                        return false;
                    }
//                    console.log('leaving 2nd $a.click')
                });
                $(linkCont).append($a);
                $(linkCont).append(historyOrRss);
                $(linkCont).append($div);
            } else {
                var aHref = 'javascript:goTo("' + encodeURI(path) + '");';
                var dirOrGear = null
                if(child.system.path == "/proc" || child.system.path.indexOf('/proc/') == 0){
                    dirOrGear = "active" ;
                } else {
                    dirOrGear = "folder" ;
                }
                var $a = $("<a class='flushLeft' mime='" + dirOrGear + "' href='" + aHref + "'>" + path.substring(path.lastIndexOf('/') + 1) + "</a>");
                $(linkCont).append($a);
                if(dirOrGear == "folder") {
                    //if not file nor link, present rss instead of history:
                    hsrc.value = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48c3ZnIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgaWQ9IlJTU2ljb24iIHZpZXdCb3g9IjAgMCA4IDgiIHdpZHRoPSIxMCIgaGVpZ2h0PSIxMCI+PHRpdGxlPlJTUyBmZWVkIGljb248L3RpdGxlPjxzdHlsZSB0eXBlPSJ0ZXh0L2NzcyI+IC5idXR0b24ge3N0cm9rZTogbm9uZTsgZmlsbDogI2ZmODAwMDt9IC5zeW1ib2wge3N0cm9rZTogbm9uZTsgZmlsbDogd2hpdGU7fTwvc3R5bGU+PHJlY3QgICBjbGFzcz0iYnV0dG9uIiB3aWR0aD0iOCIgaGVpZ2h0PSI4IiByeD0iMS41IiAvPjxjaXJjbGUgY2xhc3M9InN5bWJvbCIgY3g9IjIiIGN5PSI2IiByPSIxIiAvPjxwYXRoICAgY2xhc3M9InN5bWJvbCIgZD0ibSAxLDQgYSAzLDMgMCAwIDEgMywzIGggMSBhIDQsNCAwIDAgMCAtNCwtNCB6IiAvPjxwYXRoICAgY2xhc3M9InN5bWJvbCIgZD0ibSAxLDIgYSA1LDUgMCAwIDEgNSw1IGggMSBhIDYsNiAwIDAgMCAtNiwtNiB6IiAvPjwvc3ZnPgo=";
                    htitle.value = "RSS";
                    historyOrRss.href = path + "?format=atom&op=search&length=50";
                    $(linkCont).append(historyOrRss);
                }
            }
            linkCont.appendTo('#directories');
        }
    }

    var contentHeight = $('#objTable').height() + $('#sysTable').height() + 300;
    $('.container').height(contentHeight);

    var s = window.location.protocol + '//' + window.location.hostname + (window.location.port ? ':' + window.location.port : '');
    // Update links

    $('#linkToCurrent').attr('href', infoton.system.path)
    $('#linkToHistory').attr('href', infoton.system.path + '?with-history')
    //$('.url-link a').attr('href', infoton.system.path).text(s + infoton.system.path)
    $('#linkToAtom').attr('href', infoton.system.path + '?format=atom&op=search&length=50')
    
    if (supports_history_api()) {
    	window.history.replaceState({url: window.location.href, oldstate: window.history.state}, null, infoton.system.path);
    }
//    console.log('leaving renderInfoton')
}

function supports_history_api() {
//	console.log('entering & leaving supports_history_api')
	return !!(window.history && history.replaceState);
}

    
//var isLinkregEx = new RegExp(/^((((H|h)(T|t)|(F|f))(T|t)(P|p)((S|s)?))\:\/\/)?(((www.|[a-zA-Z0-9].)[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,6})|(localhost))(\:[0-9]{1,5})*(\/($|[a-zA-Z0-9\.\,\;\?\'\\\+&amp;%\$#\=~_\-]+))*$/);

/* Checks weather this path is a link.
 returns the link while adding 'http' if protocol is missing or null if not a link
 */
function isLink(path) {
//	console.log('entering & leaving isLink')
	if(path.slice(0, 7).toLowerCase() == 'http://' || path.slice(0, 8).toLowerCase() == 'https://')
		return path;
	if(path.slice(0,4).toLowerCase() == 'www.') 
		return 'http://' + path;
	// if got here, now a link, return null
	return null;
} 



/*
function isLink(path) {
    if (path.substring(0, 4).toLowerCase() == 'http') {
        return path;
    }
    var reply = null;
    var groups = isLinkregEx.exec(path);
    if (groups != null) {
        reply = groups[0];
        if (groups[2] == null) {
            reply = 'http://' + reply;
        }
    }
    return reply
}
*/

function isText(mimeType) {
//	console.log('entering & leaving isText')
    if(mimeType.indexOf('text')!=-1 || mimeType.indexOf('xml')!=-1 || mimeType.indexOf('json')!=-1) {
        return true;
    } else {
        return false;
    }
}

function isMarkdown(mimeType) {
    if(mimeType.indexOf('text/x-markdown')!=-1 || mimeType.indexOf('text/vnd.daringfireball.markdown')!=-1){
        return true;
    } else {
        return false;
    }
}

//defined as css selectors in styles.css
function isMimeknownToCmwell(mt) {
    if( mt.indexOf('image')!=-1 ||
        mt.indexOf('file')!=-1 ||
        mt.indexOf('presentation')!=-1 ||
        mt.indexOf('powerpoint')!=-1 ||
        mt.indexOf('excel')!=-1 ||
        mt.indexOf('spreadsheet')!=-1 ||
        mt.indexOf('word')!=-1 ||
        mt.indexOf('javascript')!=-1 ||
        mt.indexOf('pdf')!=-1 ||
        mt.indexOf('xml')!=-1){
        return true;
    } else {
        return false;
    }
}

function getParent() {
//	console.log('entering getParent')
    var parent = infoton.system.path.substring(0, infoton.system.path.lastIndexOf("/"));
//    console.log('leaving getParent')
    return parent;
}

function createUploader() {
//	console.log('entering createUploader')
    var uploader = new qq.FileUploader({
        element: document.getElementById('file_uploader_div'),
        action: window.location.pathname,
        debug: true
    });
//    console.log('leaving createUploader')
}

