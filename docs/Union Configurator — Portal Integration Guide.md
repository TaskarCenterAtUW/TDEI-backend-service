# Union Configurator — Portal Integration Guide

Oct 29, 2026 · @Mahesh

The configurator has one job: produce the union `config`. The portal mounts it as a component, gives it a starting config, and gets the full config back on every change. The portal sends that `config` unchanged with the union job; everything else stays with the portal.

## Component

```ts
import { mount } from './union-configurator.js';   // TDEI-backend-service: union-configurator/dist/

mount(element, { initialConfig?, readOnly?, onChange(config, valid) })
  → { getConfig(), setConfig(config), setReadOnly(readOnly), destroy() }
```

## Inputs

| Input | Type | Meaning |
| --- | --- | --- |
| `initialConfig` | config object, optional | Settings to start from; omitted = recommended defaults |
| `readOnly` | boolean, optional | Show the settings without allowing edits |
| `setConfig(config)` | method | Replace the current settings |
| `setReadOnly(readOnly)` | method | Switch read-only on or off |
| `destroy()` | method | Remove the component; call on unmount |

## Outputs

| Output | Type | Meaning |
| --- | --- | --- |
| `onChange(config, valid)` | event | Fires on every edit with the full config, and `valid` = every value is complete and in range |
| `getConfig()` | method | Returns the current config |

## React usage

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

- Uses the portal's CSS variables from `src/index.css` (`--brand-primary`, `--tdei-blue`, `--primary-font-family`, …); it has no theme of its own.
- All its CSS is scoped under its own root class; it adds nothing global to the portal.
