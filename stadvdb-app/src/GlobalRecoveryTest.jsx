import React, { useState } from 'react';
import { AlertCircle, CheckCircle, Loader, RefreshCw, Trash2, Play } from 'lucide-react';
import './GlobalRecoveryTest.css';

const GlobalRecoveryTest = () => {
  const [loading, setLoading] = useState(false);
  const [selectedCase, setSelectedCase] = useState(null);
  const [response, setResponse] = useState(null);
  const [error, setError] = useState('');
  const [caseResults, setCaseResults] = useState({});

  const API_BASE_URL = 'http://localhost:5000/api';

  const testCases = [
    {
      id: 'case1',
      title: 'Run Case 1',
      description: 'Replication Failure - Node 2 to Central',
      endpoint: '/recovery/test/case1',
      color: '#3b82f6'
    },
    {
      id: 'case2',
      title: 'Run Case 2',
      description: 'Central Node Recovery',
      endpoint: '/recovery/test/case2',
      color: '#8b5cf6'
    },
    {
      id: 'case3',
      title: 'Run Case 3',
      description: 'Replication Failure - Central to Node 2',
      endpoint: '/recovery/test/case3',
      color: '#ec4899'
    },
    {
      id: 'case4',
      title: 'Run Case 4',
      description: 'Node 2 Recovery',
      endpoint: '/recovery/test/case4',
      color: '#f59e0b'
    }
  ];

  const handleRunCase = async (caseId, endpoint) => {
    setLoading(true);
    setError('');
    setResponse(null);
    setSelectedCase(caseId);

    try {
      const res = await fetch(`${API_BASE_URL}${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        }
      });

      const data = await res.json();
      if (res.ok) {
        setResponse(data);
        setCaseResults(prev => ({
          ...prev,
          [caseId]: { success: true, data }
        }));
      } else {
        setError(data.error || 'Operation failed');
        setCaseResults(prev => ({
          ...prev,
          [caseId]: { success: false, error: data.error }
        }));
      }
    } catch (err) {
      setError(`Network error: ${err.message}`);
      setCaseResults(prev => ({
        ...prev,
        [caseId]: { success: false, error: err.message }
      }));
    } finally {
      setLoading(false);
    }
  };

  const handleRunAllCases = async () => {
    setLoading(true);
    setError('');
    setResponse(null);
    setSelectedCase('all');

    try {
      const res = await fetch(`${API_BASE_URL}/recovery/test/all`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        }
      });

      const data = await res.json();
      if (res.ok) {
        setResponse(data);
        setCaseResults({
          case1: { success: true },
          case2: { success: true },
          case3: { success: true },
          case4: { success: true }
        });
      } else {
        setError(data.error || 'Operation failed');
      }
    } catch (err) {
      setError(`Network error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleCleanup = async () => {
    setLoading(true);
    setError('');
    setResponse(null);
    setSelectedCase('cleanup');

    try {
      const res = await fetch(`${API_BASE_URL}/recovery/test/cleanup`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        }
      });

      const data = await res.json();
      if (res.ok) {
        setResponse(data);
        setCaseResults({});
      } else {
        setError(data.error || 'Cleanup failed');
      }
    } catch (err) {
      setError(`Network error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleMarkFailure = async (node) => {
    setLoading(true);
    setError('');
    setResponse(null);
    setSelectedCase(`fail-${node}`);

    try {
      const res = await fetch(`${API_BASE_URL}/recovery/mark/${node}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });

      const data = await res.json();
      if (res.ok) {
        setResponse(data);
      } else {
        setError(data.error || 'Failed to mark node as down');
      }
    } catch (err) {
      setError(`Network error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const formatRecoveryResult = (result) => {
    if (!result) return null;

    return (
      <div className="recovery-result-details">
        {result.case && <p><strong>Case:</strong> {result.case}</p>}
        {result.message && <p><strong>Message:</strong> {result.message}</p>}
        
        {result.result && (
          <div className="result-breakdown">
            <h4>Recovery Statistics:</h4>
            {typeof result.result === 'object' ? (
              <div className="result-items">
                {Object.entries(result.result).map(([key, value]) => (
                  <div key={key} className="result-item">
                    <strong>{key}:</strong>
                    {typeof value === 'object' ? (
                      <div className="nested-result">
                        {Object.entries(value).map(([k, v]) => (
                          <div key={k} className="nested-item">
                            <span>{k}:</span>
                            <span>Synced: {v.synced}, Skipped: {v.skipped}</span>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <span>Synced: {value.synced}, Skipped: {value.skipped}</span>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <p>Synced: {result.result.synced}, Skipped: {result.result.skipped}</p>
            )}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="global-recovery-test">
      <div className="recovery-container">
        
        {/* Header */}
        <div className="recovery-header">
          <div className="header-content">
            <AlertCircle size={40} className="header-icon" />
            <div>
              <h1 className="header-title">Global Failure Recovery Test Menu</h1>
              <p className="header-subtitle">Test distributed database recovery scenarios</p>
            </div>
          </div>
        </div>

        {/* Failure Simulation Section */}
        <div className="failure-section">
          <h2 className="section-title">Simulate Node Failure</h2>
          <div className="failure-buttons">
            <button
              onClick={() => handleMarkFailure("central")}
              disabled={loading}
              className="fail-button central-fail"
            >
              Fail Central Node
            </button>

            <button
              onClick={() => handleMarkFailure("node2")}
              disabled={loading}
              className="fail-button node2-fail"
            >
              Fail Node 2
            </button>

            <button
              onClick={() => handleMarkFailure("node3")}
              disabled={loading}
              className="fail-button node3-fail"
            >
              Fail Node 3
            </button>
          </div>
        </div>

        {/* Test Cases Grid */}
        <div className="test-cases-section">
          <h2 className="section-title">Test Cases</h2>
          <div className="test-cases-grid">
            {testCases.map(testCase => (
              <button
                key={testCase.id}
                onClick={() => handleRunCase(testCase.id, testCase.endpoint)}
                disabled={loading}
                className={`test-case-button ${caseResults[testCase.id]?.success ? 'success' : ''} ${loading && selectedCase === testCase.id ? 'loading' : ''}`}
                style={{ borderLeftColor: testCase.color }}
              >
                <div className="case-icon">
                  {loading && selectedCase === testCase.id ? (
                    <Loader className="spinner" size={24} />
                  ) : caseResults[testCase.id]?.success ? (
                    <CheckCircle size={24} className="success-icon" />
                  ) : (
                    <Play size={24} />
                  )}
                </div>
                <div className="case-content">
                  <h3 className="case-title">{testCase.title}</h3>
                  <p className="case-description">{testCase.description}</p>
                </div>
              </button>
            ))}
          </div>
        </div>

        {/* Action Buttons */}
        <div className="action-buttons-section">
          <button
            onClick={handleRunAllCases}
            disabled={loading}
            className={`btn-run-all ${loading && selectedCase === 'all' ? 'loading' : ''}`}
          >
            {loading && selectedCase === 'all' ? (
              <>
                <Loader className="spinner" size={20} />
                Running All Cases...
              </>
            ) : (
              <>
                <Play size={20} />
                Run All Cases
              </>
            )}
          </button>

          <button
            onClick={handleCleanup}
            disabled={loading}
            className={`btn-cleanup ${loading && selectedCase === 'cleanup' ? 'loading' : ''}`}
          >
            {loading && selectedCase === 'cleanup' ? (
              <>
                <Loader className="spinner" size={20} />
                Cleaning Up...
              </>
            ) : (
              <>
                <Trash2 size={20} />
                Cleanup Test Data
              </>
            )}
          </button>
        </div>

        {/* Response Display */}
        {response && (
          <div className={`response-section ${response.success ? 'success' : 'error'}`}>
            <div className="response-header">
              {response.success ? (
                <CheckCircle size={24} className="success-icon" />
              ) : (
                <AlertCircle size={24} className="error-icon" />
              )}
              <h3 className="response-title">
                {response.success ? 'Operation Successful' : 'Operation Failed'}
              </h3>
            </div>
            <div className="response-content">
              {formatRecoveryResult(response)}
            </div>
          </div>
        )}

        {/* Error Display */}
        {error && (
          <div className="response-section error">
            <div className="response-header">
              <AlertCircle size={24} className="error-icon" />
              <h3 className="response-title">Error</h3>
            </div>
            <div className="response-content">
              <p className="error-message">{error}</p>
            </div>
          </div>
        )}

        {/* Case Results Summary */}
        {Object.keys(caseResults).length > 0 && (
          <div className="results-summary-section">
            <h3 className="summary-title">Test Results Summary</h3>
            <div className="results-grid">
              {testCases.map(testCase => (
                <div key={testCase.id} className={`result-card ${caseResults[testCase.id]?.success ? 'success' : caseResults[testCase.id] ? 'failed' : ''}`}>
                  <div className="result-icon">
                    {caseResults[testCase.id]?.success ? (
                      <CheckCircle size={20} className="success-icon" />
                    ) : caseResults[testCase.id] ? (
                      <AlertCircle size={20} className="error-icon" />
                    ) : null}
                  </div>
                  <div className="result-text">
                    <p className="result-case">{testCase.title}</p>
                    <p className="result-status">
                      {caseResults[testCase.id]?.success ? 'Passed' : caseResults[testCase.id] ? 'Failed' : 'Pending'}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default GlobalRecoveryTest;
