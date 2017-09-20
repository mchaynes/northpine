var url;
var loop;
var finished = false;

$("#document").ready(hideElements);

function hideElements() {
    // $("#greyBar").css("visibility", "hidden");
    // $("#progressBar").css("visibility", "hidden");
    // $("#download").css("visibility", "hidden");
    // $("#status").css("visibility", "hidden");
}
function displayElements() {
    $("#greyBar").css("visibility", "visible");
    $("#progressBar").css("visibility", "visible");
    $("#download").css("visibility", "visible");
    $("#status").css("visibility", "visible");
}



function start() {
    finished = false;
    url = $("#url").val();
    $.ajax({
        url: "/scrape",
        data : {
            url : url
        },
        success : function(response) {
            refreshStatus();
            loop = setInterval(refreshStatus, 500);
            $("#status").text(response);
        },
        error : function(error) {
            // alert("Please enter a url");
            // console.log(error);
        }
    })
}

function refreshStatus() {
    $.getJSON("/status", {url : url }, function(response) {
        console.log(response);
        finished = response.finished;

        if( finished === true) {
            clearInterval(loop);
            $("#status").text("Finished!");
            $("#download").prop("disabled", false);
        }
        else {
            $("#download").prop("disabled", true);
            displayElements();
            var progressBar =$("#progressBar");
            var percentDone = 100 * response.done / response.total;
            progressBar.css("width", percentDone + "%");
            var status = $("#status");
            status.text( response.layer + ": " + Math.round(percentDone) + "%" );
        }
        console.log((response.done / response.total) + "%");
    })
}

function download() {
    if(finished) {
        window.location.replace("/output?url=" + url);
    }
}