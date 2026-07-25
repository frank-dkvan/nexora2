import React, { useState, useEffect } from 'react';
import { Sidebar } from './Sidebar';
import { DashboardPage } from '../../pages/DashboardPage';
import { GraphBrowserPage } from '../../pages/GraphBrowserPage';
import { CypherPage } from '../../pages/CypherPage';
import { StandingQueriesPage } from '../../pages/StandingQueriesPage';
import { IngestPage } from '../../pages/IngestPage';
import { MetricsPage } from '../../pages/MetricsPage';
import { VectorSearchPage } from '../../pages/VectorSearchPage';
import { MaterializedViewPage } from '../../pages/MaterializedViewPage';
import { ClusterTopologyPage } from '../../pages/ClusterTopologyPage';
import { SlowQueriesPage } from '../../pages/SlowQueriesPage';
import { SQVisualBuilderPage } from '../../pages/SQVisualBuilderPage';
import { ExplainPage } from '../../pages/ExplainPage';
import { api } from '../../lib';
import { HealthResponse } from '../../types';

export const AppLayout: React.FC = () => {
  const [activePage, setActivePage] = useState('dashboard');
  const [online, setOnline] = useState(false);

  useEffect(() => {
    const check = () => {
      api.get<HealthResponse>('/api/v2/health')
        .then((h) => setOnline(h.status === 'healthy'))
        .catch(() => setOnline(false));
    };
    check();
    const id = setInterval(check, 5000);
    return () => clearInterval(id);
  }, []);

  const renderPage = () => {
    switch (activePage) {
      case 'dashboard': return <DashboardPage />;
      case 'explorer': return <GraphBrowserPage />;
      case 'cypher': return <CypherPage />;
      case 'explain': return <ExplainPage />;
      case 'sq': return <StandingQueriesPage />;
      case 'sq-builder': return <SQVisualBuilderPage />;
      case 'ingest': return <IngestPage />;
      case 'metrics': return <MetricsPage />;
      case 'vector': return <VectorSearchPage />;
      case 'mv': return <MaterializedViewPage />;
      case 'cluster': return <ClusterTopologyPage />;
      case 'slow-queries': return <SlowQueriesPage />;
      default: return <DashboardPage />;
    }
  };

  return (
    <div className="d-flex" style={{ minHeight: '100vh' }}>
      <Sidebar activePage={activePage} onNavigate={setActivePage} online={online} />
      <main className="flex-grow-1 overflow-auto">
        {renderPage()}
      </main>
    </div>
  );
};
