import { node } from "../dom.js";
import { formatTimestamp, formatBytes } from "../formatters.js";

function queryFromStructured(data) {
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

class DatasetListing extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: "open" });
        this.shadowRoot.innerHTML = "<table-view></table-view>";
        this.table = this.shadowRoot.querySelector("table-view");
    }

    async loadDatasets() {
        const raw = await (await fetch('/api/datasets')).json();
        this.datasets = Object.fromEntries(raw.map(x => [x.id, x]))
    }

    // TODO(next): consider redrawing this regularly (setInterval)
    // two reasons: 1) refresh "created time", 2) load new datasets uploaded through curl and other means

    async connectedCallback() {
        await this.loadDatasets();
        const header = ["Identifier", "Name", "Created Time", "Size (original)", "Size on disk", "Number of rows", "Table schema"];        
        const datasets = Object.values(this.datasets);
        datasets.sort((a, b) => b.created_timestamp - a.created_timestamp)
        const rows = [];

        for (const ds of datasets) {
            // ARCH: this foo@vbar should be a function or something
            const query = queryFromStructured({dataset: `${ds.name}@v${ds.id}`, limit: 100});
            const cols = [
                node("a", {"href": `/query?sql=${encodeURIComponent(query)}`}, ds.id),
                ds.name,
                node("span",
                    {"title": (new Date(ds.created_timestamp / 1000 / 1000).toISOString())},
                    formatTimestamp(ds.created_timestamp / 1000 / 1000 / 1000)
                    ),
                formatBytes(ds.size_raw),
                formatBytes(ds.size_on_disk),
                ds.nrows.toLocaleString(),
                node("details", {},
                    [
                        node("summary", {}, `${ds.schema.length} columns`),
                        node("ul", {}, ds.schema.map(
                            col => node("li", {}, `${col.name} (${col.dtype})`)
                        ))
                    ]),
            ];
            rows.push(cols);
        }

        this.table.setData({
            header: header,
            rows: rows,
        })
    }
}

window.customElements.define("dataset-listing", DatasetListing);
