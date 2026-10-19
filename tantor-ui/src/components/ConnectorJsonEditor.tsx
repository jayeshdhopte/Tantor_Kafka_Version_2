import { useEffect, useRef } from 'react';
import { Compartment, EditorState } from '@codemirror/state';
import { EditorView, keymap, lineNumbers, highlightActiveLine, highlightActiveLineGutter } from '@codemirror/view';
import { json } from '@codemirror/lang-json';
import { foldGutter, foldKeymap, syntaxHighlighting, defaultHighlightStyle, bracketMatching } from '@codemirror/language';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';

export default function ConnectorJsonEditor({ value, readOnly, onChange }: { value: string; readOnly: boolean; onChange: (value: string) => void }) {
  const element = useRef<HTMLDivElement>(null);
  const editor = useRef<EditorView | null>(null);
  const editable = useRef(new Compartment());
  const initial = useRef({ value, readOnly });
  const change = useRef(onChange);
  useEffect(() => { change.current = onChange; }, [onChange]);
  useEffect(() => {
    if (!element.current) return;
    const view = new EditorView({ parent: element.current, state: EditorState.create({ doc: initial.current.value, extensions: [
      lineNumbers(), foldGutter(), highlightActiveLine(), highlightActiveLineGutter(), history(), json(), bracketMatching(),
      syntaxHighlighting(defaultHighlightStyle), keymap.of([...defaultKeymap, ...historyKeymap, ...foldKeymap]),
      editable.current.of([EditorState.readOnly.of(initial.current.readOnly), EditorView.editable.of(!initial.current.readOnly)]),
      EditorView.contentAttributes.of({ 'aria-label': 'Connector configuration JSON', role: 'textbox', 'aria-multiline': 'true' }),
      EditorView.updateListener.of(update => { if (update.docChanged) change.current(update.state.doc.toString()); }),
      EditorView.theme({ '&': { fontSize: '13px' }, '.cm-content': { minHeight: '300px', fontFamily: 'monospace' }, '.cm-scroller': { overflow: 'auto', maxHeight: '560px' }, '.cm-gutters': { backgroundColor: '#f8f6fc', color: '#667085', border: 'none' } }),
    ] }) });
    editor.current = view;
    return () => { view.destroy(); editor.current = null; };
  }, []);
  useEffect(() => { editor.current?.dispatch({ effects: editable.current.reconfigure([EditorState.readOnly.of(readOnly), EditorView.editable.of(!readOnly)]) }); }, [readOnly]);
  useEffect(() => {
    const view = editor.current;
    if (view && view.state.doc.toString() !== value) view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: value } });
  }, [value]);
  return <div className="ds-kc-code-editor" ref={element} />;
}
