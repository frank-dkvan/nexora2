import React from 'react';
import ReactDOM from 'react-dom/client';
import { AppLayout } from './components/layout';
import '@coreui/coreui/dist/css/coreui.min.css';
import './index.css';

// Bootstrap Icons are loaded via CSS from node_modules at build time.
// The icons render as inline SVG via the <i> tag class names.

const App: React.FC = () => {
  return <AppLayout />;
};

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
