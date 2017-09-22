let url;
let loop;
let finished = false;

$("#document").ready(onReady);


function onReady() {
    const element = document.querySelector('form');
    displayUrlBox();
    element.addEventListener('submit', function(event) {
        event.preventDefault();
        if($("#urlBox").val().length > 0) {
            start();
        }
        return false;
    });
}

function displayUrlBox() {
    $("#urlBox").css("display", "inline-block");
    $("#progressBar").css("display", "none");
}

function displayLoadingBar() {
    $("#urlBox").css("display", "none");
    $("#progressBar").css("display", "inline-block");
}

function start() {
    finished = false;
    event.preventDefault();
    url = $("#urlBox").val();
    $("#status").text("Getting layer details...");
    $.ajax({
        url: "/scrape",
        data : {
            url : url
        },
        success : function(response) {
            displayLoadingBar();
            refreshStatus();
            loop = setInterval(refreshStatus, 500);
            $("#status").text(response);
        },
        error : function(error) {
            // alert("Please enter a url");
            // console.log(error);
        }
    });
    return false;
}

function refreshStatus() {
    $.getJSON("/status", {url : url }, function(response) {
        finished = response.finished;
        if( finished === true) {
            clearInterval(loop);
            let status = $("#status");
            status.text("Finished!");
            $("#download").prop("disabled", false);
            let percentDone = 100 * response.done / response.total;
            $("#progressBar").find("> div").css("width", percentDone + "%");
            download();
            displayUrlBox();
            status.text("Another?");
        }
        else if(response.total !== 0 && response.total === response.done) {
            $("#status").text("Zipping up files...");
        } else if(response.total !== 0) {
            $("#download").prop("disabled", true);
            let progressBar =$("#progressBar").find("> div");
            let percentDone = 100 * response.done / response.total;
            progressBar.css("width", percentDone + "%");
            let status = $("#status");
            status.text( response.layer + ": " + Math.round(percentDone) + "%" );
        }
    })
}

function download() {
    window.location.replace("/output?url=" + url);
}