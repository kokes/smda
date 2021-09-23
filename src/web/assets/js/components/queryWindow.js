// TODO(PR): finish

// TODO(PR): the main complication is to unify state of the query itself:
// it's in three places: URL, component property, query.textarea (maybe we can merge the last two?)

import { formatBytes, formatTimestamp, formatDuration } from '../formatters.js';
import { node } from '../dom.js';

async function runQuery(query) {
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

// TODO(PR): this should be its own component
async function renderTable(data) {
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
                    tbody.innerHTML = "";
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

    return table;
}

const queryTmpl = document.createElement("template");
queryTmpl.innerHTML = `
<style type='text/css'>
textarea#sql {
    display: block;
    margin: 1em 0;
    padding: .5em;
}

div#submit-query {
    margin-top: 1em;
}
div#submit-query button {
    padding: .3em 1em;
}

div#submit-query  small#elapsed {
    padding-left: 1em;
}
</style>

<link rel="stylesheet" href="../../tables.css" />

<div id="query">
    <form action="/query" name="query">
        <textarea name="sql" id="sql" rows=10 cols=100 placeholder="SELECT * FROM foo LIMIT 100"></textarea>

        <div id="submit-query">
            <button>Run query</button>
            <small id="elapsed"></small>
        </div>
    </form>
</div>

<div id="query-results"></div>
</div>
`;

class QueryWindow extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: "open" });
        // TODO(PR): this is just pasted 1:1, edit this appropriately
        // perhaps split it into multiple components?
        this.shadowRoot.appendChild(queryTmpl.content.cloneNode(true))
    }

    connectedCallback() {
        // submit on shift-enter
        this.shadowRoot.querySelector("div#query textarea#sql").addEventListener("keydown", e => {
            if (!(e.code === "Enter" && e.shiftKey === true)) {
                return;
            }
            e.preventDefault();
            // TODO(next)/ARCH: we can't do `document.forms["query"].submit()`, because that
            // would circumvent our router
            this.shadowRoot.querySelector("div#submit-query button").click();
        });
        // TODO(PR): add this and other nodes to private properties
        this.shadowRoot.querySelector("div#submit-query button").addEventListener("click", async () => {
            const target = this.shadowRoot.getElementById("query-results");
            const elapsed = this.shadowRoot.querySelector("small#elapsed");

            const query = this.shadowRoot.querySelector("textarea").value.trim();
            if (query === "") {
                target.innerHTML = "";
                elapsed.innerHTML = "";
                return;
            }

            let data, incrementor, startTime, success = false;
            try {
                startTime = performance.now();
                incrementor = setInterval(() => {
                    elapsed.textContent = formatDuration(performance.now() - startTime, "Elapsed: ");
                }, 100)

                data = await runQuery(query);
                success = true;
            } catch(e) {
                // errDialog("Failed to run query", e)
                console.error("failed to run query" + e) // TODO(PR): refactor once we have errDialog
            } finally {
                clearInterval(incrementor);
                const runtime = formatDuration(performance.now() - startTime, "Elapsed: ");
                elapsed.textContent = `${runtime} (${formatBytes(data.bytes_read)} scanned)`;
            }

            if (!success) {
                return
            }
            const table = await renderTable(data);
            target.innerHTML = "";
            target.append(table);
        })
    }
}

window.customElements.define("query-window", QueryWindow);
