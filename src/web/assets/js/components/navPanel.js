import { node } from '../dom.js';

// ARCH/TODO: incorporate at some point? (make sure it ties in the router for back/forward to work)
// const currentClass = "current";
// for (let link of document.querySelectorAll("nav#panel ul li a")) {
//     if (link.classList.contains(currentClass)) {
//         link.classList.remove(currentClass);
//     }
//     if (link.getAttribute("href") === window.location.pathname) {
//         link.classList.add(currentClass);
//     }
// }

class NavPanel extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: "open" });
    }

    connectedCallback() {
        this.shadowRoot.innerHTML = `
        <style>
        ul {margin: 0; padding: 0;}
        ul li {
            list-style: none;
        }
        ul li a {
            color: #CCDBDC;
            display: block;
            padding: 1em;
            text-decoration: underline;
        }
        ul li a:hover, ul li a.current {
            background: darkslategray;
            pointer: hand;
        }
        </style>
        `;

        const routes = {
            "/": "Overview",
            "/query": "Query",
        };
        const links = node("ul");
        for (let [href, name] of Object.entries(routes)) {
            const link = node("a", {href: href}, name);
            link.addEventListener("click", (e) => {
                history.pushState({}, "", href);
                // TODO/ARCH: again, this is a hack (used elsewhere)
                window.onpopstate();
                e.preventDefault();
            });
            links.append(node("li", null, link));
        }
        this.shadowRoot.appendChild(links);
    }
}

window.customElements.define("nav-panel", NavPanel);
