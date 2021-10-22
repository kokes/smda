import { node } from '../dom.js';

class ErrDialog extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: "open" });
    }

    connectedCallback() {
        this.shadowRoot.innerHTML = `
        <style>
        :host {
            position: absolute;
            left: 50%; top: 10%;
            width: 400px;
        }
        div {
            padding: 1em;
            margin-bottom: .5em;
            width: 100%;
            margin-left: -50%;
            border: 1px solid red;
            background: lightpink;
            color: darkred;
        }
        h3 { margin: 0 0 .5em 0; padding: 0; }
        </style>
        `;

        document.addEventListener("keydown", e => {
            if (e.code != "Escape") {
                return;
            }
            for (let dv of this.shadowRoot.querySelectorAll("div")) {
                dv.remove();
            }
        })
    }

    addError(title, msg) {
        const err = node("div", null, [
            node("h3", null, title),
            msg,
            node("small", {style: "display: block; text-align: right; margin: -.5em; font-style: italic;"}, "Press escape to close"),
        ]);
        err.setAttribute("title", "click to dismiss");
        err.addEventListener("click", e => {
            e.target.closest("div").remove();
        });
        this.shadowRoot.appendChild(err);
    }
}

window.customElements.define("err-dialog", ErrDialog);
