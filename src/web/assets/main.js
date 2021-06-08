import { formatBytes, formatTimestamp, formatDuration } from './js/formatters.js';
import { empty, node } from './js/dom.js';

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

// ARCH: this does a bit of UI stuff - maybe separate that out completely, so that
// this becomes more testable
class smda {
    constructor() {        
        this.setupUploader();
    }
    // ARCH: move elsewhere
    setupUploader() {
        const fp = document.getElementById("filepicker");
        fp.addEventListener("change", async e => {
            e.target.disabled = "disabled";
            for (const file of e.target.files) {
                const filename = encodeURIComponent(file.name);
                const request = await fetch(`/upload/auto?name=${filename}`, {
                    method: "POST",
                    body: file,
                })
                if (request.ok !== true) {
                    errDialog(`failed to upload ${file.name}`, await request.text());
                    continue;
                }
                // ARCH/TODO: we're fetching dataset listings from the API... but we already have it in the
                // request response... maybe add it to `this.datasets` from there directly
                // this will also mean it won't be async (but we'll need to trigger the UI change)
                await this.setupDatasets();
            }
            e.target.value = "";
            e.target.disabled = "";
        })
    }
    async submitQuery() {
        const qform = document.forms["query"];
        const target = document.getElementById("query-results");
        const elapsed = qform.querySelector("small#elapsed");

        const query = Object.fromEntries(
            [...qform.querySelectorAll("input")]
                .filter(x => x.value !== "")
                .map(x => [x.name, x.value])
        )
        if (query.dataset !== undefined) {
            const valueparts = query.dataset.split("@v");
            query.dataset = {
                name: valueparts[0],
                id: valueparts[1], // ARCH: might be `version`
            }
        }
        if (query.limit !== undefined) {
            query.limit = parseInt(query.limit, 10);
        }
        
        if (Object.entries(query).length === 0) {
            empty(elapsed);
            empty(target);
            return;
        }

        let data, incrementor, startTime, success = false;
        try {
            startTime = performance.now();
            incrementor = setInterval(() => {
                elapsed.textContent = formatDuration(performance.now() - startTime, "Elapsed: ");
            }, 100)

            data = await this.runQuery(query);
            success = true;
        } catch(e) {
            errDialog("Failed to run query", e)
        } finally {
            clearInterval(incrementor);
            elapsed.textContent = formatDuration(performance.now() - startTime, "Elapsed: ");
        }

        if (success) {
            empty(target);
            this.renderTable(data, target);
        }
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
                const dsv = encodeURIComponent(`${ds.name}@v${ds.id}`);
                const cols = [
                    node("td", null, node("a", {"href": `/query?dataset=${dsv}&limit=100`}, ds.id)),
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
    async runQuery(query) {
        if (query["select"] === undefined) {
            query["select"] = "*";
        }
        if (query["limit"] === undefined) {
            query["limit"] = 100; // ARCH: safety mechanism
        }
        const req = await fetch('/api/query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(query),
        })
        if (req.ok === false) {
            const error = await req.text();
            throw new Error(error);
        }
        return await req.json();
    }
    async renderTable(data, target) {
        const table = node("table", {"class": "data-view"},
            node("thead", {},
                node("tr", {}, data.schema.map((col) => node("th", {}, col.name)))
            )
        )

        for (let rowNum=0; rowNum < data.nrows; rowNum++) {
            const rowData = data.data[rowNum];
            const row = node("tr", {},
                data.schema.map((val, idx) => {
                    return node("td", {}, rowData[idx])
                })
            )
            table.appendChild(row);
            // ARCH: I think the imperative code below is more readable...
            // const row = document.createElement('tr');
            // for (let colNum = 0; colNum < data.schema.length; colNum++) {
            //     const cell = document.createElement('td');
            //     cell.innerText = data.data[colNum][rowNum];
            //     row.appendChild(cell);
            // }
            // table.appendChild(row);
        }

        empty(target);
        target.appendChild(table);
    }
}

export { smda };
export { Router } from './js/router.js';
