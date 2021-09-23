import { formatBytes, formatTimestamp, formatDuration } from './js/formatters.js';
import { empty, node } from './js/dom.js';

import './js/components/fileUploader.js';
import './js/components/queryWindow.js';

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

class smda {
    queryFromStructured(data) {
        if (Object.entries(data).length === 0) {
            return "";
        }
        return [
            `SELECT ${data["select"] ? data["select"] : "*"}`,
            `${data["dataset"] ? "FROM " + data["dataset"] : ""}`,
            `${data["filter"] ? "WHERE " + data["filter"] : ""}`,
            `${data["aggregate"] ? "GROUP BY " + data["aggregate"] : ""}`,
            `${data["order"] ? "ORDER BY " + data["order"] : ""}`,
            `${data["limit"] !== undefined ? "LIMIT " + data["limit"] : ""}`,
        ].filter(x => x.trim() !== "").join("\n");
    }
    async setupDatasets() {
        const target = document.getElementById('route-root');

        // OPTIM: well, this is uncached in multiple places
        await this.loadDatasets();
        const header = node("thead", {}, node("tr", null, ["Identifier", "Name", "Created Time", "Size (original)", "Size on disk", "Number of rows", "Table schema"].map(
            column => node("th", null, column)
        )));
        const datasets = Object.values(this.datasets);
        datasets.sort((a, b) => b.created_timestamp - a.created_timestamp)
        const rows = node("tbody", null, datasets.map(
            ds => {
                // ARCH: this foo@vbar should be a function or something
                const query = this.queryFromStructured({dataset: `${ds.name}@v${ds.id}`, limit: 100});
                const cols = [
                    node("td", null, node("a", {"href": `/query?sql=${encodeURIComponent(query)}`}, ds.id)),
                    node("td", null, ds.name),
                    node("td", null,
                        node("span",
                            {"title": (new Date(ds.created_timestamp / 1000 / 1000).toISOString())},
                            formatTimestamp(ds.created_timestamp / 1000 / 1000 / 1000)
                            )
                        ),
                    node("td", null, formatBytes(ds.size_raw)),
                    node("td", null, formatBytes(ds.size_on_disk)),
                    node("td", null, ds.nrows.toLocaleString()),
                    node("td", null, node("details", {},
                    [
                        node("summary", {}, `${ds.schema.length} columns`),
                        node("ul", {}, ds.schema.map(
                            col => node("li", {}, `${col.name} (${col.dtype})`)
                        ))
                    ])),
                ]
                return node("tr", null, cols);
            }
        ));
        const overview = node("table", {"id": "datasets"}, [header, rows]);

        // ARCH: target.innerHTML = overview.outerHTML;
        empty(target);
        target.append(overview);
    }
    // OPTIM: perhaps abort if loaded in the past n miliseconds
    async loadDatasets() {
        const raw = await (await fetch('/api/datasets')).json();
        this.datasets = Object.fromEntries(raw.map(x => [x.id, x]))
    }
}

export { smda };
export { Router } from './js/router.js';
