import { formatBytes, formatTimestamp, formatDuration } from './js/formatters.js';
import { empty, node } from './js/dom.js';

import './js/components/fileUploader.js';
import './js/components/queryWindow.js';
import './js/components/tableView.js';
import './js/components/datasetListing.js';

// TODO(PR): move this to the errDialog component
document.addEventListener("keydown", e => {
    if (e.code != "Escape") {
        return;
    }
    for (let dv of document.querySelectorAll("div#errors div")) {
        dv.remove();
    }
})
function errDialog(title, msg) {
    const target = document.getElementById("errors");
    target.setAttribute("title", "click to dismiss");
    const name = node("h3", null, title);
    const err = node("div", null, [name, msg]);
    err.addEventListener("click", e => {
        e.target.closest("div").remove();
    });
    target.append(err);
}

// TODO(PR): this won't be needed as we don't pass anything to it?
export { Router } from './js/router.js';
