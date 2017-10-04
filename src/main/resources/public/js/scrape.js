let url;
let loop;
let finished = false;
let alreadyRanDownload = false;

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
    $.getJSON("/status", {url : url }, updateStatus);
}

/**
 *
 * @param status {{finished: boolean, failed: boolean, layer: string, total: number, done: number, errorMessage: string}}
 */
function updateStatus(status) {
    finished = status.finished;
    if(status.failed === true) {
        clearInterval(loop);
        $("#status").text("Failed :(");
        alert(status.errorMessage);
    } else if( finished === true && !alreadyRanDownload ) {
        clearInterval(loop);
        let status = $("#status");
        status.text("Finished!");
        $("#download").prop("disabled", false);
        let percentDone = 100 * status.done / status.total;
        $("#progressBar").find("> div").css("width", percentDone + "%");
        download();
        displayUrlBox();
        status.text("Another?");
    } else if(status.total !== 0 && status.total === status.done) {
        $("#status").text("Zipping up files...");
    } else if(status.total !== 0) {
        $("#download").prop("disabled", true);
        let progressBar =$("#progressBar").find("> div");
        let percentDone = 100 * status.done / status.total;
        progressBar.css("width", percentDone + "%");
        $(  "#status").text( status.layer + ": " + Math.round(percentDone) + "%" );
    }
}



function download() {
    if(!alreadyRanDownload) {
        alreadyRanDownload = true;
        window.location.replace("/output?url=" + url);
    }
}