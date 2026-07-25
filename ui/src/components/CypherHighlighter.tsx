import React, { useRef, useEffect } from 'react';

// Cypher keywords for syntax highlighting
const KEYWORDS = new Set([
  'MATCH', 'OPTIONAL', 'WHERE', 'RETURN', 'ORDER', 'BY', 'SKIP', 'LIMIT',
  'CREATE', 'MERGE', 'DELETE', 'SET', 'REMOVE', 'ON', 'CREATE', 'MATCH',
  'WITH', 'UNWIND', 'UNION', 'ALL', 'CALL', 'YIELD', 'IN', 'AS', 'AND',
  'OR', 'NOT', 'XOR', 'IS', 'NULL', 'TRUE', 'FALSE', 'DISTINCT',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'EXISTS', 'COUNT', 'SUM', 'AVG',
  'MIN', 'MAX', 'COLLECT', 'SIZE', 'STARTS', 'ENDS', 'CONTAINS',
  'LOAD', 'CSV', 'FROM', 'HEADERS', 'FIELDTERMINATOR', 'CONSTRAINT',
  'ASSERT', 'INDEX', 'FOR', 'DETACH', 'PROFILE', 'EXPLAIN', 'USING',
  'PERIODIC', 'COMMIT', 'FOREACH', 'SHORTEST', 'PATH', 'ANY', 'SHORTESTPATH',
  'ALLSHORTESTPATHS', 'SINGLE', 'NONE',
]);

const FUNCTIONS = new Set([
  'count', 'sum', 'avg', 'min', 'max', 'collect', 'size', 'length',
  'type', 'id', 'labels', 'keys', 'properties', 'nodes', 'relationships',
  'head', 'tail', 'last', 'toInteger', 'toFloat', 'toString', 'toBoolean',
  'abs', 'ceil', 'floor', 'round', 'sign', 'sqrt', 'rand',
  'sin', 'cos', 'tan', 'asin', 'acos', 'atan', 'atan2', 'pi', 'e', 'log', 'log10', 'exp',
  'left', 'right', 'substring', 'trim', 'ltrim', 'rtrim', 'split', 'replace',
  'reverse', 'toUpperCase', 'toLowerCase', 'replace',
  'date', 'datetime', 'time', 'timestamp', 'duration',
]);

interface Token {
  text: string;
  type: 'keyword' | 'function' | 'string' | 'number' | 'comment' | 'operator' | 'variable' | 'label' | 'plain';
}

function tokenize(line: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;

  while (i < line.length) {
    const char = line[i];

    // Whitespace
    if (/\s/.test(char)) {
      let ws = '';
      while (i < line.length && /\s/.test(line[i])) ws += line[i++];
      tokens.push({ text: ws, type: 'plain' });
      continue;
    }

    // Comment (// ...)
    if (char === '/' && line[i + 1] === '/') {
      tokens.push({ text: line.substring(i), type: 'comment' });
      break;
    }

    // String (single or double quotes)
    if (char === '"' || char === "'") {
      const quote = char;
      let str = char;
      i++;
      while (i < line.length && line[i] !== quote) {
        if (line[i] === '\\' && i + 1 < line.length) {
          str += line[i++] + line[i++];
        } else {
          str += line[i++];
        }
      }
      if (i < line.length) str += line[i++];
      tokens.push({ text: str, type: 'string' });
      continue;
    }

    // Number
    if (/\d/.test(char)) {
      let num = '';
      while (i < line.length && /[\d.]/.test(line[i])) num += line[i++];
      tokens.push({ text: num, type: 'number' });
      continue;
    }

    // Label/Type (:Label or :REL_TYPE)
    if (char === ':' && /[A-Za-z_]/.test(line[i + 1] || '')) {
      let label = ':';
      i++;
      while (i < line.length && /[A-Za-z0-9_]/.test(line[i])) label += line[i++];
      tokens.push({ text: label, type: 'label' });
      continue;
    }

    // Variable/identifier
    if (/[A-Za-z_]/.test(char)) {
      let word = '';
      while (i < line.length && /[A-Za-z0-9_]/.test(line[i])) word += line[i++];

      // Check if it's a keyword (case-insensitive)
      const upper = word.toUpperCase();
      if (KEYWORDS.has(upper)) {
        tokens.push({ text: word, type: 'keyword' });
      } else if (line[i] === '(') {
        // Function call
        tokens.push({ text: word, type: 'function' });
      } else {
        tokens.push({ text: word, type: 'variable' });
      }
      continue;
    }

    // Operators and punctuation
    if (/[<>!=+\-*/%&|^~]/.test(char)) {
      let op = '';
      while (i < line.length && /[<>!=+\-*/%&|^~]/.test(line[i])) op += line[i++];
      tokens.push({ text: op, type: 'operator' });
      continue;
    }

    // Everything else
    tokens.push({ text: char, type: 'plain' });
    i++;
  }

  return tokens;
}

const TOKEN_COLORS: Record<Token['type'], string> = {
  keyword: '#8b5cf6',      // purple
  function: '#3b82f6',     // blue
  string: '#10b981',       // green
  number: '#f59e0b',       // amber
  comment: '#94a3b8',      // gray
  operator: '#ef4444',     // red
  variable: '#1e293b',     // dark
  label: '#06b6d4',        // cyan
  plain: '#475569',        // slate
};

interface CypherHighlighterProps {
  value: string;
  onChange: (value: string) => void;
  onRun?: () => void;
  rows?: number;
  placeholder?: string;
}

export const CypherHighlighter: React.FC<CypherHighlighterProps> = ({
  value,
  onChange,
  onRun,
  rows = 4,
  placeholder,
}) => {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const overlayRef = useRef<HTMLPreElement>(null);

  // Sync scroll between textarea and overlay
  const handleScroll = () => {
    if (textareaRef.current && overlayRef.current) {
      overlayRef.current.scrollTop = textareaRef.current.scrollTop;
      overlayRef.current.scrollLeft = textareaRef.current.scrollLeft;
    }
  };

  // Render highlighted tokens
  const renderHighlighted = () => {
    const lines = value.split('\n');
    return lines.map((line, lineIdx) => {
      const tokens = tokenize(line);
      return (
        <div key={lineIdx}>
          {tokens.map((token, i) => (
            <span key={i} style={{ color: TOKEN_COLORS[token.type], fontWeight: token.type === 'keyword' ? 600 : 400 }}>
              {token.text}
            </span>
          ))}
          {lineIdx < lines.length - 1 && '\n'}
        </div>
      );
    });
  };

  return (
    <div className="position-relative" style={{ height: `${rows * 1.5}rem` }}>
      {/* Highlighted overlay */}
      <pre
        ref={overlayRef}
        aria-hidden="true"
        className="position-absolute top-0 start-0 w-100 h-100 m-0 p-2 font-monospace"
        style={{
          fontSize: '0.85rem',
          lineHeight: '1.5rem',
          overflow: 'auto',
          pointerEvents: 'none',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
          background: 'transparent',
          border: '1px solid transparent',
          zIndex: 1,
        }}
      >
        {renderHighlighted()}
        {' '}
      </pre>
      {/* Transparent textarea on top */}
      <textarea
        ref={textareaRef}
        className="form-control font-monospace position-absolute top-0 start-0 w-100 h-100"
        style={{
          fontSize: '0.85rem',
          lineHeight: '1.5rem',
          background: 'transparent',
          color: 'transparent',
          caretColor: '#475569',
          border: '1px solid var(--cui-border-color, #dee2e6)',
          zIndex: 2,
          resize: 'none',
        }}
        rows={rows}
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        onScroll={handleScroll}
        onKeyDown={(e) => {
          if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            e.preventDefault();
            onRun?.();
          }
        }}
        spellCheck={false}
        autoComplete="off"
        autoCorrect="off"
        autoCapitalize="off"
      />
    </div>
  );
};
