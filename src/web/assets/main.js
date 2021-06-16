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
        this.setupColumnFilter()
    }
    // ARCH: move elsewhere (both UI setups)
    // ARCH: maybe move this one to submitQuery?
    setupColumnFilter() {
        const dsi = document.querySelector("input#dataset");
        dsi.addEventListener("change", async e => {
            if (this.datasets === undefined) {
                await this.loadDatasets(); // OPTIM: we might want to load metadata for just one dataset
            }
            if (e.target.value === "") {
                return;
            }
            const version = e.target.value.split("@v")[1];
            const ds = this.datasets[version];
            const target = document.getElementById("column-filter");
            const inner = document.createElement("div");
            const filter = document.createElement("input");
            filter.setAttribute("placeholder", "filter columns");
            filter.addEventListener("keyup", e => {
                const needle = e.target.value;
                // TODO(next): add S/A/F links - add to select, aggregate, filter fields (and be aware of its contents)
                for (const col of e.target.parentNode.querySelectorAll("ul li")) {
                    const val = col.textContent;
                    // OPTIM: we can be smart about this and save our last query and if it's a subset, we can skip some columns etc.
                    if (needle === "" || val.includes(needle)) {
                        col.style.display = "list-item";
                    } else {
                        col.style.display = "none";
                    }
                }
            });

            inner.append(filter);

            const columns = document.createElement("ul");

            for (const col of ds.schema) {
                columns.append(node("li", null, `${col.name} (${col.dtype})`));
            }

            inner.append(columns);
            // we cannot do `target.innerHTML = ...`, because we're registering event listeners
            empty(target);
            target.append(inner);
        });

        // trigger this upon first load
        dsi.dispatchEvent(new Event("change"));
    }
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
            // ARCH/TODO: create a helper function to work with dataset versions (search `@v` for more use cases here)
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
            const runtime = formatDuration(performance.now() - startTime, "Elapsed: ");
            elapsed.textContent = `${runtime} (${formatBytes(data.bytes_read)} scanned)`;
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
                node("tr", {}, data.schema.map((col, idx) => {
                    const th = node("th", {}, col.name);
                    if (data.ordering[idx] !== null) {
                        const pictos = {"asc": "↑", "desc": "↓"};
                        th.append(node("span", {"class": "ordering"}, pictos[data.ordering[idx]]))
                    }

                    return th;
                }))
            )
        )

        for (let rowNum=0; rowNum < data.nrows; rowNum++) {
            const rowData = data.data[rowNum];
            const row = node("tr", {},
                data.schema.map((_, idx) => {
                    let val = rowData[idx];
                    if (typeof(val) === "number" && !Number.isInteger(val)) {
                        // ARCH: why three? what if we need more precision?
                        val = val.toFixed(3);
                        // trim trailing zeroes... it's a bit clunky at the moment, but I guess it's better than a regex
                        // TODO: test - 0, 100, 20.00, 0.00, 2.34, 2.340, 2.00, 2.001, 234, ...
                        if (val.endsWith("0") && val.length > 1 && val.includes(".")) {
                            for (let j=val.length-1; j >= 0; j--) {
                                const char = val.charAt(j);
                                if (char === "." || char !== "0") {
                                    val = val.slice(0, j + 1 - (char === "."));
                                    break;
                                }
                            }
                        }
                    }
                    return node("td", {}, val);
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
