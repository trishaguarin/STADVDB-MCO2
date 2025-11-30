import React, { useState } from 'react';
import { Database, Plus, Search, Edit, Trash2, Server, CheckCircle, XCircle, Loader } from 'lucide-react';
import './App.css';

const ConcurrencyWarnings = ({ concurrencyInfo }) => {
  if (!concurrencyInfo) return null;

  const {
    isolation_warnings,
    phantom_warning,
    dirty_read_warning,
    non_repeatable_warning,
    concurrency_note
  } = concurrencyInfo;

  const warnings = [];

  if (isolation_warnings) {
    warnings.push({
      title: `Isolation Level: ${isolation_warnings.isolation_level}`,
      list: isolation_warnings.warnings
    });
  }

  if (dirty_read_warning)
    warnings.push({ title: dirty_read_warning.type, list: [dirty_read_warning.message] });

  if (non_repeatable_warning)
    warnings.push({ title: non_repeatable_warning.type, list: [non_repeatable_warning.message] });

  if (phantom_warning)
    warnings.push({ title: phantom_warning.type, list: [phantom_warning.message] });

  if (concurrency_note)
    warnings.push({ title: concurrency_note.type, list: [concurrency_note.message] });

  if (warnings.length === 0) return null;

  return (
    <div className="concurrency-warnings">
      <h3>⚠️ Concurrency Warnings</h3>
      {warnings.map((w, i) => (
        <div key={i} className="warning-card">
          <h4>{w.title}</h4>
          <ul>
            {w.list.map((msg, j) => (
              <li key={j}>{msg}</li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
};

const formatResponse = (response, operation) => {
  if (!response) return null;

  const transactionTimeDisplay = response.transaction_time_ms ? (
    <div className="transaction-time">
      <strong>Transaction Time:</strong> {response.transaction_time_ms.toFixed(2)} ms
    </div>
  ) : null;

  if (operation === 'read' && response.results) {
    return (
      <div className="read-response">
        {transactionTimeDisplay}
        
        {/* Show warnings for READ operations */}
        {response.concurrency_info && (
          <ConcurrencyWarnings concurrencyInfo={response.concurrency_info} />
        )}

        <div className="node-result">
          <h4>Central Node (All Data)</h4>
          {response.results.central.status === 'found' ? (
            <div className="node-data">
              <p><strong>Order ID:</strong> {response.results.central.data.orderID}</p>
              <p><strong>Delivery Date:</strong> {new Date(response.results.central.data.deliveryDate).toLocaleDateString()}</p>
            </div>
          ) : (
            <p>No data found in central node</p>
          )}
        </div>

        <div className="node-result">
          <h4>Node 2 (2024 Data)</h4>
          {response.results.node2.status === 'found' ? (
            <div className="node-data">
              <p><strong>Order ID:</strong> {response.results.node2.data.orderID}</p>
              <p><strong>Delivery Date:</strong> {new Date(response.results.node2.data.deliveryDate).toLocaleDateString()}</p>
            </div>
          ) : response.results.node2.status === 'unavailable' ? (
            <p className="text-warning">Node unavailable</p>
          ) : (
            <p>No data found in this node</p>
          )}
        </div>

        <div className="node-result">
          <h4>Node 3 (2025 Data)</h4>
          {response.results.node3.status === 'found' ? (
            <div className="node-data">
              <p><strong>Order ID:</strong> {response.results.node3.data.orderID}</p>
              <p><strong>Delivery Date:</strong> {new Date(response.results.node3.data.deliveryDate).toLocaleDateString()}</p>
            </div>
          ) : response.results.node3.status === 'unavailable' ? (
            <p className="text-warning">Node unavailable</p>
          ) : (
            <p>No data found in this node</p>
          )}
        </div>
      </div>
    );
  }


  if (response.message) {
    return (
    <div className="operation-response">
      {transactionTimeDisplay}
      
      {/* Show concurrency warnings */}
      {response.concurrency_info && (
        <ConcurrencyWarnings concurrencyInfo={response.concurrency_info} />
      )}

      <p className="success-message">{response.message}</p>
        {response.results && (
          <div className="operation-details">
            {Object.entries(response.results).map(([node, result]) => (
              <div key={node} className="node-result">
                <h4>{node.toUpperCase()}:</h4>
                <p><strong>Status:</strong> {result.status}</p>
                {result.message && <p>{result.message}</p>}
                {result.data && (
                  <div className="node-data">
                    <p><strong>Order ID:</strong> {result.data.orderID}</p>
                    {result.data.deliveryDate && (
                      <p><strong>Delivery Date:</strong> {new Date(result.data.deliveryDate).toLocaleDateString()}</p>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  return <pre>{JSON.stringify(response, null, 2)}</pre>;
};

const App = () => {
  const [isolationLevel, setIsolationLevel] = useState('');
  const [operation, setOperation] = useState('');
  const [orderId, setOrderId] = useState('');
  const [deliveryDate, setDeliveryDate] = useState('');
  const [loading, setLoading] = useState(false);
  const [response, setResponse] = useState(null);
  const [transactionTime, setTransactionTime] = useState(null);
  const [error, setError] = useState('');

  const isolationLevels = {
    '1': 'READ UNCOMMITTED',
    '2': 'READ COMMITTED',
    '3': 'REPEATABLE READ',
    '4': 'SERIALIZABLE'
  };

  const API_BASE_URL = 'http://localhost:5000/api';

  const handleSubmit = async () => {
    if (!orderId || (operation !== 'read' && operation !== 'delete' && !deliveryDate)) {
      setError('Please fill in all required fields');
      return;
    }

    setLoading(true);
    setError('');
    setResponse(null);

    try {
      const endpoint = `${API_BASE_URL}/${operation}`;
      const body = {
        isolation_level: isolationLevels[isolationLevel],
        order_id: parseInt(orderId)
      };

      if (operation === 'insert' || operation === 'update') {
        body.delivery_date = deliveryDate;
      }

      const res = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body)
      });

      const data = await res.json();
      
      if (res.ok) {
        setResponse(data);
      } else {
        setError(data.error || 'Operation failed');
      }
    } catch (err) {
      setError(`Network error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const resetForm = () => {
    setOrderId('');
    setDeliveryDate('');
    setResponse(null);
    setError('');
  };

  const handleOperationChange = (op) => {
    setOperation(op);
    resetForm();
  };

  return (
    <div className="app">
      <div className="container">
        {/* Header */}
        <header className="header">
          <div className="header-content">
            <Database size={40} className="header-icon" />
            <div>
              <h1 className="header-title">Distributed Database Management System</h1>
              <p className="header-subtitle">STADVDB MCO2 - Multi-Node Transaction Manager</p>
            </div>
          </div>
        </header>

        {/* Node Status */}
        <div className="node-status">
          <div className="node-card">
            <Server size={20} />
            <div>
              <div className="node-name">Central Node</div>
              <div className="node-info">10.2.14.120:3306</div>
            </div>
            <CheckCircle size={20} className="status-icon status-active" />
          </div>
          <div className="node-card">
            <Server size={20} />
            <div>
              <div className="node-name">Node 2 (2024)</div>
              <div className="node-info">10.2.14.121:3306</div>
            </div>
            <CheckCircle size={20} className="status-icon status-active" />
          </div>
          <div className="node-card">
            <Server size={20} />
            <div>
              <div className="node-name">Node 3 (2025)</div>
              <div className="node-info">10.2.14.122:3306</div>
            </div>
            <CheckCircle size={20} className="status-icon status-active" />
          </div>
        </div>

        {/* Isolation Level Selection */}
        {!isolationLevel && (
          <div className="section">
            <h2 className="section-title">Select Isolation Level</h2>
            <div className="isolation-grid">
              {Object.entries(isolationLevels).map(([key, value]) => (
                <button
                  key={key}
                  onClick={() => setIsolationLevel(key)}
                  className="isolation-button"
                >
                  <div className="isolation-number">{key}</div>
                  <div className="isolation-name">{value}</div>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Operation Selection */}
        {isolationLevel && !operation && (
          <div className="section">
            <div className="section-header">
              <h2 className="section-title">
                Isolation Level: {isolationLevels[isolationLevel]}
              </h2>
              <button onClick={() => setIsolationLevel('')} className="btn-secondary">
                Change Level
              </button>
            </div>
            
            <h3 className="operation-title">Select Operation</h3>
            <div className="operation-grid">
              <button onClick={() => handleOperationChange('insert')} className="operation-button">
                <Plus size={32} />
                <span>Insert Order</span>
              </button>
              <button onClick={() => handleOperationChange('read')} className="operation-button">
                <Search size={32} />
                <span>Read Order</span>
              </button>
              <button onClick={() => handleOperationChange('update')} className="operation-button">
                <Edit size={32} />
                <span>Update Order</span>
              </button>
              <button onClick={() => handleOperationChange('delete')} className="operation-button">
                <Trash2 size={32} />
                <span>Delete Order</span>
              </button>
            </div>
          </div>
        )}

        {/* Operation Form */}
        {isolationLevel && operation && (
          <div className="section">
            <div className="section-header">
              <h2 className="section-title">
                {operation.charAt(0).toUpperCase() + operation.slice(1)} Order
              </h2>
              <button onClick={() => setOperation('')} className="btn-secondary">
                Back to Operations
              </button>
            </div>

            <div className="form">
              <div className="form-group">
                <label htmlFor="orderId" className="form-label">Order ID</label>
                <input
                  id="orderId"
                  type="number"
                  value={orderId}
                  onChange={(e) => setOrderId(e.target.value)}
                  placeholder="Enter Order ID"
                  className="form-input"
                />
              </div>

              {(operation === 'insert' || operation === 'update') && (
                <div className="form-group">
                  <label htmlFor="deliveryDate" className="form-label">Delivery Date</label>
                  <input
                    id="deliveryDate"
                    type="date"
                    value={deliveryDate}
                    onChange={(e) => setDeliveryDate(e.target.value)}
                    className="form-input"
                  />
                  <p className="form-hint">
                    Year 2024 → Node 2 | Year 2025 → Node 3
                  </p>
                </div>
              )}

              <div className="form-actions">
                <button
                  onClick={handleSubmit}
                  disabled={loading}
                  className="btn-primary"
                >
                  {loading ? (
                    <>
                      <Loader className="spinner" size={20} />
                      Processing...
                    </>
                  ) : (
                    `Execute ${operation.charAt(0).toUpperCase() + operation.slice(1)}`
                  )}
                </button>
                <button
                  onClick={resetForm}
                  className="btn-secondary"
                >
                  Clear
                </button>
              </div>
            </div>

            {/* Response Display */}
            {response && (
              <div className="response response-success">
                <CheckCircle size={20} />
                <div className="response-content">
                  <div className="response-title">Operation Successful</div>
                  <div className="response-details">
                    {formatResponse(response, operation)}
                  </div>
                </div>
              </div>
            )}

            {/* Error Display */}
            {error && (
              <div className="response response-error">
                <XCircle size={20} />
                <div>
                  <div className="response-title">Error</div>
                  <div className="response-content">{error}</div>
                </div>
              </div>
            )}
          </div>
        )}

       
      </div>

    </div>
  );
};

export default App;