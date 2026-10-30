# Union Configurator

Builds the `config` of a union 5.0 job. Its only job is to produce that config; the host
application owns datasets, sign-in and submitting the job.

## Use

```js
import { mount } from './union-configurator.js';

const ui = mount(element, {
  initialConfig,                 // optional: settings to start from
  readOnly: false,               // optional
  onChange(config, valid) { },   // after every change
});

ui.getConfig();      // the current config
ui.setConfig(cfg);   // replace the settings
ui.destroy();        // on unmount
```

React:

```jsx
function UnionConfigurator({ initialConfig, readOnly, onChange }) {
  const el = useRef(null), handle = useRef(null), cb = useRef(onChange);
  cb.current = onChange;
  useEffect(() => {
    handle.current = mount(el.current, { initialConfig, readOnly, onChange: (c, v) => cb.current(c, v) });
    return () => handle.current.destroy();
  }, []);
  useEffect(() => { if (initialConfig) handle.current?.setConfig(initialConfig); }, [initialConfig]);
  useEffect(() => { handle.current?.setReadOnly(!!readOnly); }, [readOnly]);
  return <div ref={el} />;
}
```

## Styling

- Uses the host page's CSS variables, each with a fallback: `--brand-primary`, `--brand-accent`,
  `--secondary-color`, `--tdei-blue`, `--tdei-green`, `--purple-background-light`,
  `--purple-background-medium`, `--purple-background-dark`, `--primary-font-family`,
  `--secondary-font-family`.
- Loads no fonts; the host page provides them.
- All of its CSS is scoped under `.u5c` and added once as `<style id="u5c-style">`, removed when the last
  instance is destroyed.

## Build

```bash
npm run build
```

`src/union-configurator.js` is the source. The build writes `dist/union-configurator.js` (the module) and
`dist/union-configurator.html` (the standalone page, which adds loading a pasted config and a Copy bar).

There are no dataset fields: the host picks the two datasets, and the configurator always calls them DS1
(dataset one of the job) and DS2 (dataset two).

## Changing a setting

A new or changed union setting goes to `union_5_0.sql` and its test harness first, then
`docs/union-FAQs.md`, then this configurator. Raise `version` in `package.json` with each release.
