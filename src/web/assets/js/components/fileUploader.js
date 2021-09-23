// TODO(PR): finish

class FileUploader extends HTMLElement {
    constructor() {
        super();
        this.innerHTML = "<input id='filepicker' type='file' multiple />";
        // TODO(PR): shouldn't the event listener be on something else?
        this.addEventListener("change", async (e) => {
            e.target.disabled = "disabled";
            for (const file of e.target.files) {
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
            e.target.value = "";
            e.target.disabled = "";
        });
    }
}

window.customElements.define("file-uploader", FileUploader);