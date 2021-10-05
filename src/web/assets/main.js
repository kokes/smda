import './js/components/navPanel.js';
import './js/components/fileUploader.js';
import './js/components/queryWindow.js';
import './js/components/tableView.js';
import './js/components/datasetListing.js';
import './js/components/errDialog.js';

function router() {
    const path = window.location.pathname.slice(1).split("/");
    let route = path[0];
    if (route === "") {
        route = "root";
    }
    document.querySelectorAll("main > div").forEach(x => x.style.display = "none");
    // ARCH: pass this down to submitQuery/setupDatasets?
    const target = document.getElementById(`route-${route}`);
    if (target !== null) {
        target.style.display = "block";
    }
    // TODO(PR): move this elsewhere - or perhaps trigger .route() on all webcomponents we have
    if (route === "query") {
        const params = new URLSearchParams(window.location.search);
        const query = params.get("sql");
        // TODO(PR): is there a better way to tie components together?
        document.querySelector("query-window").updateQuery(query);
    }
}

window.onload = () => router();
window.onpopstate = () => router();
