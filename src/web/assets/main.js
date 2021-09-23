import { formatBytes, formatTimestamp, formatDuration } from './js/formatters.js';
import { empty, node } from './js/dom.js';

import './js/components/fileUploader.js';

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
        this.setupQueryWindow()
    }
    // ARCH: move elsewhere (all UI setups) / TODO(PR): remove this
    setupQueryWindow() {
        // submit on shift-enter
        document.querySelector("div#query textarea#sql").addEventListener("keydown", e => {
            if (!(e.code === "Enter" && e.shiftKey === true)) {
                return;
            }
            e.preventDefault();
            // TODO(next)/ARCH: we can't do `document.forms["query"].submit()`, because that
            // would circumvent our router
            document.forms["query"].querySelector("button").click();
        });
    }
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
    async submitQuery() {
        const qform = document.forms["query"];
        const target = document.getElementById("query-results");
        const elapsed = qform.querySelector("small#elapsed");

        const query = qform.sql.value.trim();
        if (query === "") {
            empty(target);
            empty(elapsed);
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
    async runQuery(query) {
        const req = await fetch('/api/query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({"sql": query}),
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
                    const props = {"data-idx": idx, "data-dtype": col.dtype};
                    if (data.ordering[idx] !== null) {
                        props["data-ordering"] = data.ordering[idx];
                    }
                    const th = node("th", props, col.name);
                    th.addEventListener("click", e => {
                        const dtype = e.target.getAttribute("data-dtype");
                        const isNumeric = dtype === "float" || dtype === "int"; // TODO: isNumeric as a function?
                        const existing = e.target.getAttribute("data-ordering");
                        let newOrder = "asc";
                        if (existing === "asc") {
                            newOrder = "desc";
                        }
                        const ths = e.target.closest("thead").querySelectorAll("tr th");
                        ths.forEach(x => x.removeAttribute("data-ordering"));
                        e.target.setAttribute("data-ordering", newOrder);

                        const colIdx = parseInt(e.target.getAttribute("data-idx"), 10);
                        const tbody = e.target.closest("table").querySelector("tbody");
                        const trs = Array.from(tbody.querySelectorAll("tr"));

                        const ltv = newOrder === "asc" ? -1 : 1;
                        trs.sort((a, b) => {
                            const cells = [a, b].map(x => x.children[colIdx]);
                            let vals = cells.map(x => x.textContent);
                            const nulls = cells.map(x => x.getAttribute("data-null") !== null);
                            if (nulls[0] && nulls[1]) {
                                return 0;
                            }
                            if (nulls[0] || nulls[1]) {
                                // nulls first for asc, nulls last for desc (mapping [0, 1] to [-1, 1])
                                return (2*nulls[0] - 1)*ltv;
                            }
                            if (isNumeric) {
                                vals = vals.map(x => parseFloat(x));
                            }
                            if (vals[0] === vals[1]) { return 0; }
                            if (vals[0] < vals[1]) {  return ltv; }
                            return -ltv;
                        });
                        empty(tbody);
                        trs.forEach(x => tbody.append(x));
                    });

                    return th;
                }))
            )
        )

        const tbody = node("tbody", null);
        for (let rowNum=0; rowNum < data.nrows; rowNum++) {
            const rowData = data.data[rowNum];
            const row = node("tr", {},
                data.schema.map((_, idx) => {
                    let val = rowData[idx];
                    let props = {};
                    if (val === null) {
                        props["data-null"] = "null";
                        val = "";
                    }
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
                    return node("td", props, val);
                })
            )
            tbody.appendChild(row);
            // ARCH: I think the imperative code below is more readable...
            // const row = document.createElement('tr');
            // for (let colNum = 0; colNum < data.schema.length; colNum++) {
            //     const cell = document.createElement('td');
            //     cell.innerText = data.data[colNum][rowNum];
            //     row.appendChild(cell);
            // }
            // table.appendChild(row);
        }
        table.append(tbody);

        empty(target);
        target.appendChild(table);
    }
}

export { smda };
export { Router } from './js/router.js';
