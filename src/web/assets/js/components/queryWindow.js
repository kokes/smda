// TODO(PR): finish

// TODO(PR): the main complication is to unify state of the query itself:
// it's in three places: URL, component property, query.textarea (maybe we can merge the last two?)

class QueryWindow extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: "open" });
        // TODO(PR): this is just pasted 1:1, edit this appropriately
        // perhaps split it into multiple components?
        // TODO(PR): move CSS here
        this.shadowRoot.innerHTML = `
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
    }

    connectedCallback() {
        // submit on shift-enter
        this.shadowRoot.querySelector("div#query textarea#sql").addEventListener("keydown", e => {
            if (!(e.code === "Enter" && e.shiftKey === true)) {
                return;
            }
            console.log("submitti")
            e.preventDefault();
            // TODO(next)/ARCH: we can't do `document.forms["query"].submit()`, because that
            // would circumvent our router
            this.shadowRoot.querySelector("div#submit-query button").click();
        });
    }
}

window.customElements.define("query-window", QueryWindow);

// const qform = document.forms["query"];
// const target = document.getElementById("query-results");
// const elapsed = qform.querySelector("small#elapsed");

// const query = qform.sql.value.trim();
// if (query === "") {
//     empty(target);
//     empty(elapsed);
//     return;
// }

// let data, incrementor, startTime, success = false;
// try {
//     startTime = performance.now();
//     incrementor = setInterval(() => {
//         elapsed.textContent = formatDuration(performance.now() - startTime, "Elapsed: ");
//     }, 100)

//     data = await this.runQuery(query);
//     success = true;
// } catch(e) {
//     errDialog("Failed to run query", e)
// } finally {
//     clearInterval(incrementor);
//     const runtime = formatDuration(performance.now() - startTime, "Elapsed: ");
//     elapsed.textContent = `${runtime} (${formatBytes(data.bytes_read)} scanned)`;
// }

// if (success) {
//     empty(target);
//     this.renderTable(data, target);
// }