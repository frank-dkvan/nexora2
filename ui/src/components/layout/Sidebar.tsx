import React from 'react';

interface SidebarProps {
  activePage: string;
  onNavigate: (page: string) => void;
  online: boolean;
}

interface NavGroup {
  label: string;
  items: NavItem[];
}

interface NavItem {
  page: string;
  icon: string;
  label: string;
}

const NAV_GROUPS: NavGroup[] = [
  {
    label: 'Overview',
    items: [
      { page: 'dashboard', icon: 'bi-speedometer2', label: 'Dashboard' },
      { page: 'metrics', icon: 'bi-bar-chart', label: 'Metrics' },
      { page: 'slow-queries', icon: 'bi-hourglass-split', label: 'Slow Queries' },
      { page: 'cluster', icon: 'bi-diagram-3', label: 'Cluster' },
    ],
  },
  {
    label: 'Query',
    items: [
      { page: 'cypher', icon: 'bi-code-slash', label: 'Cypher Query' },
      { page: 'explain', icon: 'bi-diagram-2', label: 'Query Plan' },
    ],
  },
  {
    label: 'Graph',
    items: [
      { page: 'explorer', icon: 'bi-diagram-3', label: 'Graph Browser' },
      { page: 'vector', icon: 'bi-vector-pen', label: 'Vector Search' },
      { page: 'mv', icon: 'bi-table', label: 'Mat. Views' },
    ],
  },
  {
    label: 'Automation',
    items: [
      { page: 'sq', icon: 'bi-bell', label: 'Standing Queries' },
      { page: 'sq-builder', icon: 'bi-diagram-2', label: 'SQ Builder' },
      { page: 'ingest', icon: 'bi-cloud-upload', label: 'Data Ingest' },
    ],
  },
];

export const Sidebar: React.FC<SidebarProps> = ({ activePage, onNavigate, online }) => {
  return (
    <div className="sidebar d-flex flex-column flex-shrink-0">
      <div className="d-flex align-items-center flex-shrink-0 px-3 py-2 border-bottom">
        <span className="fs-5 fw-semibold text-info">Nexora</span>
        <small className="ms-2 text-secondary">v0.7.0</small>
      </div>
      <nav className="flex-grow-1 mt-1" style={{ overflowY: 'auto' }}>
        {NAV_GROUPS.map((group) => (
          <div key={group.label} className="mb-2">
            <small className="text-secondary text-uppercase px-3" style={{ fontSize: '0.65rem', fontWeight: 600 }}>
              {group.label}
            </small>
            <ul className="nav nav-pills flex-column px-2 mt-1">
              {group.items.map((item) => (
                <li className="nav-item" key={item.page}>
                  <button
                    className={`nav-link text-start w-100 ${activePage === item.page ? 'active' : ''}`}
                    onClick={() => onNavigate(item.page)}
                  >
                    <i className={`${item.icon} me-2`}></i>
                    {item.label}
                  </button>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </nav>
      <div className="px-3 py-2 border-top text-secondary" style={{ fontSize: '0.75rem' }}>
        <span
          className="d-inline-block rounded-circle me-1"
          style={{
            width: 8,
            height: 8,
            background: online ? '#4ade80' : '#f87171',
          }}
        />
        {online ? 'Online' : 'Offline'}
      </div>
    </div>
  );
};
