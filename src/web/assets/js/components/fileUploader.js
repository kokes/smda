// TODO(PR): finish

class FileUploader extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: "open" });
        this.shadowRoot.innerHTML = "<input type='file' multiple />";
    }

    connectedCallback() {
        const filepicker = this.shadowRoot.querySelector("input[type=file]");
        // TODO(PR): shouldn't the event listener be on something else?
        filepicker.addEventListener("change", async (e) => {
            const fp = e.target;
            fp.disabled = "disabled";
            for (const file of fp.files) {
                const filename = encodeURIComponent(file.name);
                const request = await fetch(`/upload/auto?name=${filename}`, {
                    method: "POST",
                    body: file,
                })
                if (request.ok !== true) {
                    // errDialog(`failed to upload ${file.name}`, await request.text());
                    // TODO(PR): we don't have errDialog defined
                    console.error(`failed to upload ${file.name}`, await request.text())
                    continue;
                }
                // ARCH/TODO: we're fetching dataset listings from the API... but we already have it in the
                // request response... maybe add it to `this.datasets` from there directly
                // this will also mean it won't be async (but we'll need to trigger the UI change)
                // TODO(PR): chain the components somehow
                // await this.setupDatasets();
            }
            fp.value = "";
            fp.disabled = "";
        });
    }
}

window.customElements.define("file-uploader", FileUploader);