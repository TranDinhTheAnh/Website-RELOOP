import { copyFile, mkdir, readFile } from "node:fs/promises";
import { Script } from "node:vm";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const sourcePath = resolve(root, "index.html");
const outputPath = resolve(root, "dist", "index.html");
const html = await readFile(sourcePath, "utf8");
const scriptTags = html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi);
let checkedScripts = 0;

for (const [index, match] of [...scriptTags].entries()) {
  const source = match[1].trim();
  if (!source) continue;
  new Script(source, { filename: `index-inline-${index}.js` });
  checkedScripts += 1;
}

await mkdir(dirname(outputPath), { recursive: true });
await copyFile(sourcePath, outputPath);
console.log(`Built static app to dist/index.html (${checkedScripts} scripts checked).`);
