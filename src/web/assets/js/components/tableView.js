// TODO(PR): remove data-view classes everywhere

import { node } from "../dom.js";

// TODO(PR): include this in formatters?
// if (typeof(val) === "number" && !Number.isInteger(val)) {
//     // ARCH: why three? what if we need more precision?
//     val = val.toFixed(3);
//     // trim trailing zeroes... it's a bit clunky at the moment, but I guess it's better than a regex
//     // TODO: test - 0, 100, 20.00, 0.00, 2.34, 2.340, 2.00, 2.001, 234, 0.000001 (cannot round down to zero) ...
//     if (val.endsWith("0") && val.length > 1 && val.includes(".")) {
//         for (let j=val.length-1; j >= 0; j--) {
//             const char = val.charAt(j);
//             if (char === "." || char !== "0") {
//                 val = val.slice(0, j + 1 - (char === "."));
//                 break;
//             }
//         }
//     }
// }


//     th.addEventListener("click", e => {
//         const dtype = e.target.getAttribute("data-dtype");
//         const isNumeric = dtype === "float" || dtype === "int"; // TODO: isNumeric as a function?
//         const existing = e.target.getAttribute("data-ordering");
//         let newOrder = "asc";
//         if (existing === "asc") {
//             newOrder = "desc";
//         }
//         const ths = e.target.closest("thead").querySelectorAll("tr th");
//         ths.forEach(x => x.removeAttribute("data-ordering"));
//         e.target.setAttribute("data-ordering", newOrder);
    
//         const colIdx = parseInt(e.target.getAttribute("data-idx"), 10);
//         const tbody = e.target.closest("table").querySelector("tbody");
//         const trs = Array.from(tbody.querySelectorAll("tr"));
    
//         const ltv = newOrder === "asc" ? -1 : 1;
//         trs.sort((a, b) => {
//             const cells = [a, b].map(x => x.children[colIdx]);
//             let vals = cells.map(x => x.textContent);
//             const nulls = cells.map(x => x.getAttribute("data-null") !== null);
//             if (nulls[0] && nulls[1]) {
//                 return 0;
//             }
//             if (nulls[0] || nulls[1]) {
//                 // nulls first for asc, nulls last for desc (mapping [0, 1] to [-1, 1])
//                 return (2*nulls[0] - 1)*ltv;
//             }
//             if (isNumeric) {
//                 vals = vals.map(x => parseFloat(x));
//             }
//             if (vals[0] === vals[1]) { return 0; }
//             if (vals[0] < vals[1]) {  return ltv; }
//             return -ltv;
//         });
//         tbody.innerHTML = "";
//         trs.forEach(x => tbody.append(x));
//     });



class TableView extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({mode: "open"});
    }

    setData(data) {
        const {header, rows, ordering, dtypes} = data;

        const thead = node("thead", null, node("tr", null, 
            header.map((col, idx) => {
                const props = {"data-idx": idx};
                if (dtypes !== undefined) {
                    props["data-dtype"] = dtypes[idx];
                }
                if (ordering !== undefined && ordering[idx] !== null) {
                    props["data-ordering"] = ordering[idx];
                }
                return node("th", props, col)      
            })
        ));

        const tbody = node("tbody", null, rows.map(x => {
            const row = node("tr", null, x.map(val => {
                let props = {};
                if (val === null) {
                    props["data-null"] = "null";
                    val = "";
                }
                return node("td", props, val)
            }));
            return row;
        }));
        
        const table = node("table", null, [thead, tbody]);

        this.shadowRoot.innerHTML = "<link rel='stylesheet' href='../../tables.css' />";
        this.shadowRoot.appendChild(table);
    }

    connectedCallback() {
        // TODO(PR): remove?
    }
}

window.customElements.define("table-view", TableView)
