import React from 'react';
import ReactDOM from 'react-dom/client';
// Self-hosted fonts (pinned @fontsource packages): no font CDN requests from the public site
import '@fontsource-variable/hanken-grotesk';
import '@fontsource-variable/martian-mono/wdth.css';
import App from './App.jsx';
import './App.css';

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
