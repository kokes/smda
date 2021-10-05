import { formatBytes, formatDuration } from '../formatters.js';

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

div#submit-query small#elapsed {
    padding-left: 1em;
}
</style>

<link rel="stylesheet" href="../../tables.css" />


<form action="/query" name="query">
    <textarea name="sql" id="sql" rows=10 cols=100 placeholder="SELECT * FROM foo LIMIT 100"></textarea>

    <div id="submit-query">
        <button>Run query</button>
        <small id="elapsed"></small>
    </div>
</form>
`;

class QueryWindow extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: "open" });
        // TODO(PR): this is just pasted 1:1, edit this appropriately
        // perhaps split it into multiple components?
        this.shadowRoot.appendChild(queryTmpl.content.cloneNode(true));
    }

    updateQuery(query) {
        this.shadowRoot.querySelector("textarea#sql").value = query;
        // TODO(PR): cleanup any potential results?
    }

    connectedCallback() {
        // submit on shift-enter
        this.shadowRoot.querySelector("textarea#sql").addEventListener("keydown", e => {
            if (!(e.code === "Enter" && e.shiftKey === true)) {
                return;
            }
            e.preventDefault();
            // TODO(next)/ARCH: we can't do `document.forms["query"].submit()`, because that
            // would circumvent our router
            this.shadowRoot.querySelector("div#submit-query button").click();
        });
        // TODO(PR): add this and other nodes to private properties
        this.shadowRoot.querySelector("div#submit-query button").addEventListener("click", async (e) => {
            // route first
            // ARCH: encapsulate it in some generic handler?
            e.preventDefault();
            const url = new URL(window.location);
            url.search = '';
            const qform = this.shadowRoot.querySelector("form[name=query]");
            for (let entry of (new FormData(qform)).entries()) {
                url.searchParams.set(entry[0], entry[1]);
            }
            history.pushState({}, "", url);

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
                document.querySelector("err-dialog").addError("Failed to run query", e);
            } finally {
                clearInterval(incrementor);
                const runtime = formatDuration(performance.now() - startTime, "Elapsed: ");
                elapsed.textContent = `${runtime} (${formatBytes(data.bytes_read)} scanned)`;
            }

            if (!success) {
                return
            }
            this.parentNode.querySelector("table-view").setData({
                header: data.schema.map(x => x.name),
                dtypes: data.schema.map(x => x.dtype),
                ordering: data.ordering,
                rows: data.data,
            });
        })
    }
}

window.customElements.define("query-window", QueryWindow);
