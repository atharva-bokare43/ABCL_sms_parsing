import React, { useState, useEffect } from 'react';
import { 
  ChevronDown, Upload, RefreshCw, AlertCircle, 
  FilePlus, FileText, Check, X, Activity 
} from 'lucide-react';

const FinancialDashboard = () => {
  const API_BASE_URL = 'http://localhost:8000/api';
  
  const [selectedFile, setSelectedFile] = useState(null);
  const [processing, setProcessing] = useState(false);
  const [processingStatus, setProcessingStatus] = useState({
    total: 0,
    processed: 0,
    succeeded: 0,
    failed: 0,
    status: 'idle'
  });
  const [summaryData, setSummaryData] = useState({});
  const [recentMessages, setRecentMessages] = useState([]);
  const [recentErrors, setRecentErrors] = useState([]);
  const [uploadResponse, setUploadResponse] = useState(null);
  const [statsExpanded, setStatsExpanded] = useState(true);
  const [messagesExpanded, setMessagesExpanded] = useState(false);
  const [errorsExpanded, setErrorsExpanded] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(null);

  // Fetch initial data on component mount
  useEffect(() => {
    fetchDashboardData();
    return () => {
      if (refreshInterval) {
        clearInterval(refreshInterval);
      }
    };
  }, []);

  // Start auto-refresh when processing begins
  useEffect(() => {
    if (processing) {
      const interval = setInterval(() => {
        fetchProcessingStatus();
        if (processingStatus.status === 'completed' || processingStatus.status === 'failed') {
          setProcessing(false);
          fetchDashboardData();
          clearInterval(interval);
        }
      }, 2000);
      setRefreshInterval(interval);
      return () => clearInterval(interval);
    }
  }, [processing, processingStatus.status]);

  const fetchDashboardData = async () => {
    try {
      // Fetch summary data
      const summaryResponse = await fetch(`${API_BASE_URL}/dashboard/summary`);
      const summary = await summaryResponse.json();
      setSummaryData(summary);

      // Fetch recent messages
      const messagesResponse = await fetch(`${API_BASE_URL}/messages/recent?limit=10`);
      const messages = await messagesResponse.json();
      setRecentMessages(messages);

      // Fetch recent errors
      const errorsResponse = await fetch(`${API_BASE_URL}/errors/recent?limit=10`);
      const errors = await errorsResponse.json();
      setRecentErrors(errors);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
    }
  };

  const fetchProcessingStatus = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/process-status`);
      const status = await response.json();
      setProcessingStatus(status);
    } catch (error) {
      console.error('Error fetching processing status:', error);
    }
  };

  const handleFileChange = (event) => {
    setSelectedFile(event.target.files[0]);
  };

  const handleFileUpload = async () => {
    if (!selectedFile) {
      alert('Please select a file first');
      return;
    }

    try {
      const formData = new FormData();
      formData.append('file', selectedFile);
      formData.append('delimiter', ',');
      formData.append('has_header', 'true');

      const response = await fetch(`${API_BASE_URL}/upload-csv`, {
        method: 'POST',
        body: formData,
      });

      const result = await response.json();
      setUploadResponse(result);
      
      if (result.status === 'success') {
        setProcessing(true);
        fetchProcessingStatus();
      }
    } catch (error) {
      console.error('Error uploading file:', error);
      setUploadResponse({
        status: 'error',
        message: `Error uploading file: ${error.message}`
      });
    }
  };

  const handleProcessExistingFile = async () => {
    const filePath = prompt('Enter the path to the CSV file on the server:');
    if (!filePath) return;

    try {
      const formData = new FormData();
      formData.append('file_path', filePath);

      const response = await fetch(`${API_BASE_URL}/process-csv-file`, {
        method: 'POST',
        body: formData,
      });

      const result = await response.json();
      setUploadResponse(result);
      
      if (result.status === 'success') {
        setProcessing(true);
        fetchProcessingStatus();
      }
    } catch (error) {
      console.error('Error processing file:', error);
      setUploadResponse({
        status: 'error',
        message: `Error processing file: ${error.message}`
      });
    }
  };

  const handleRefresh = () => {
    fetchDashboardData();
    fetchProcessingStatus();
  };

  const renderProgressBar = () => {
    const percentage = processingStatus.total > 0 
      ? Math.floor((processingStatus.processed / processingStatus.total) * 100) 
      : 0;
    
    return (
      <div className="mt-4">
        <div className="flex justify-between mb-1">
          <span className="text-sm font-medium">Processing Progress</span>
          <span className="text-sm font-medium">{percentage}%</span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-2.5">
          <div 
            className="bg-blue-600 h-2.5 rounded-full" 
            style={{ width: `${percentage}%` }}
          ></div>
        </div>
        <div className="flex justify-between mt-1 text-xs text-gray-500">
          <span>Processed: {processingStatus.processed}/{processingStatus.total}</span>
          <span>Success: {processingStatus.succeeded} | Failed: {processingStatus.failed}</span>
        </div>
      </div>
    );
  };

  const formatCurrency = (amount) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 0
    }).format(amount);
  };

  const formatDate = (isoString) => {
    if (!isoString) return 'N/A';
    try {
      const date = new Date(isoString);
      return date.toLocaleDateString('en-IN', {
        day: '2-digit',
        month: 'short',
        year: 'numeric'
      });
    } catch (error) {
      return isoString;
    }
  };

  const renderSummaryStats = () => {
    return (
      <div className="mb-6">
        <div 
          className="flex justify-between items-center p-4 bg-gray-100 rounded-t cursor-pointer"
          onClick={() => setStatsExpanded(!statsExpanded)}
        >
          <h3 className="text-lg font-semibold">Summary Statistics</h3>
          <ChevronDown className={`w-5 h-5 transition-transform ${statsExpanded ? 'rotate-180' : ''}`} />
        </div>
        
        {statsExpanded && (
          <div className="p-4 border border-gray-200 rounded-b grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {/* Salary Section */}
            {summaryData.salary && (
              <div className="bg-white p-4 rounded shadow">
                <h4 className="text-lg font-medium text-blue-700">Salary Deposits</h4>
                <p className="text-sm text-gray-600">Total Transactions: {summaryData.salary.transaction_count}</p>
                <p className="text-2xl font-bold mt-2">{formatCurrency(summaryData.salary.total_amount)}</p>
                <p className="text-sm text-gray-600 mt-3">Highest Salary: {formatCurrency(summaryData.salary.highest_salary)}</p>
                <div className="text-xs text-gray-500 mt-2">
                  {summaryData.salary.date_range.from && (
                    <p>Period: {formatDate(summaryData.salary.date_range.from)} to {formatDate(summaryData.salary.date_range.to)}</p>
                  )}
                </div>
              </div>
            )}
            
            {/* EMI Section */}
            {summaryData.emi && (
              <div className="bg-white p-4 rounded shadow">
                <h4 className="text-lg font-medium text-red-700">EMI Payments</h4>
                <p className="text-sm text-gray-600">Total Transactions: {summaryData.emi.transaction_count}</p>
                <p className="text-2xl font-bold mt-2">{formatCurrency(summaryData.emi.total_amount)}</p>
                <p className="text-sm text-gray-600 mt-3">Unique Loans: {summaryData.emi.unique_loans}</p>
              </div>
            )}
            
            {/* Credit Card Section */}
            {summaryData.credit_card && (
              <div className="bg-white p-4 rounded shadow">
                <h4 className="text-lg font-medium text-purple-700">Credit Card Spending</h4>
                <p className="text-sm text-gray-600">Total Transactions: {summaryData.credit_card.transaction_count}</p>
                <p className="text-2xl font-bold mt-2">{formatCurrency(summaryData.credit_card.total_spent)}</p>
                <p className="text-sm text-gray-600 mt-3">Highest Outstanding: {formatCurrency(summaryData.credit_card.highest_outstanding)}</p>
              </div>
            )}
            
            {/* SIP Section */}
            {summaryData.sip && (
              <div className="bg-white p-4 rounded shadow">
                <h4 className="text-lg font-medium text-green-700">SIP Investments</h4>
                <p className="text-sm text-gray-600">Total Transactions: {summaryData.sip.transaction_count}</p>
                <p className="text-2xl font-bold mt-2">{formatCurrency(summaryData.sip.total_invested)}</p>
                <p className="text-sm text-gray-600 mt-3">Unique Folios: {summaryData.sip.unique_folios}</p>
              </div>
            )}
            
            {/* General Transactions */}
            {summaryData.general && (
              <div className="bg-white p-4 rounded shadow">
                <h4 className="text-lg font-medium text-gray-700">Other Transactions</h4>
                <p className="text-sm text-gray-600">Total Transactions: {summaryData.general.transaction_count}</p>
                <div className="grid grid-cols-2 gap-2 mt-2">
                  <div>
                    <p className="text-sm text-gray-600">Credits</p>
                    <p className="text-xl font-bold text-green-600">{formatCurrency(summaryData.general.total_credits)}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-600">Debits</p>
                    <p className="text-xl font-bold text-red-600">{formatCurrency(summaryData.general.total_debits)}</p>
                  </div>
                </div>
              </div>
            )}
            
            {/* Error Section */}
            {summaryData.errors && (
              <div className="bg-white p-4 rounded shadow">
                <h4 className="text-lg font-medium text-orange-700">Processing Errors</h4>
                <div className="flex items-center justify-between">
                  <p className="text-2xl font-bold mt-2">{summaryData.errors.count}</p>
                  <AlertCircle className="w-6 h-6 text-orange-500" />
                </div>
                <p className="text-sm text-gray-600 mt-3">Total errors during processing</p>
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  const renderRecentMessages = () => {
    return (
      <div className="mb-6">
        <div 
          className="flex justify-between items-center p-4 bg-gray-100 rounded-t cursor-pointer"
          onClick={() => setMessagesExpanded(!messagesExpanded)}
        >
          <h3 className="text-lg font-semibold">Recent Processed Messages</h3>
          <ChevronDown className={`w-5 h-5 transition-transform ${messagesExpanded ? 'rotate-180' : ''}`} />
        </div>
        
        {messagesExpanded && (
          <div className="border border-gray-200 rounded-b overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Message</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Processed At</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {recentMessages.map(message => (
                    <tr key={message.id}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{message.id}</td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full 
                          ${message.type === 'SALARY_CREDIT' ? 'bg-blue-100 text-blue-800' : 
                            message.type === 'EMI_PAYMENT' ? 'bg-red-100 text-red-800' : 
                            message.type === 'CREDIT_CARD_TRANSACTION' ? 'bg-purple-100 text-purple-800' : 
                            message.type === 'SIP_INVESTMENT' ? 'bg-green-100 text-green-800' : 
                            'bg-gray-100 text-gray-800'}`}>
                          {message.type}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-500">
                        <div className="max-w-md truncate">{message.message}</div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {formatDate(message.processed_at)}
                      </td>
                    </tr>
                  ))}
                  {recentMessages.length === 0 && (
                    <tr>
                      <td colSpan="4" className="px-6 py-4 text-center text-sm text-gray-500">
                        No messages processed yet
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderRecentErrors = () => {
    return (
      <div className="mb-6">
        <div 
          className="flex justify-between items-center p-4 bg-gray-100 rounded-t cursor-pointer"
          onClick={() => setErrorsExpanded(!errorsExpanded)}
        >
          <h3 className="text-lg font-semibold">Recent Processing Errors</h3>
          <ChevronDown className={`w-5 h-5 transition-transform ${errorsExpanded ? 'rotate-180' : ''}`} />
        </div>
        
        {errorsExpanded && (
          <div className="border border-gray-200 rounded-b overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Error</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Message</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {recentErrors.map(error => (
                    <tr key={error.id}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{error.id}</td>
                      <td className="px-6 py-4 text-sm text-red-500">
                        <div className="max-w-xs truncate">{error.error}</div>
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-500">
                        <div className="max-w-md truncate">{error.message}</div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {formatDate(error.created_at)}
                      </td>
                    </tr>
                  ))}
                  {recentErrors.length === 0 && (
                    <tr>
                      <td colSpan="4" className="px-6 py-4 text-center text-sm text-gray-500">
                        No processing errors
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="bg-white shadow-lg rounded-lg overflow-hidden">
        <div className="p-6 bg-blue-600 text-white">
          <div className="flex justify-between items-center">
            <h2 className="text-2xl font-bold">Financial SMS Analyzer Dashboard</h2>
            <button 
              onClick={handleRefresh}
              className="p-2 rounded hover:bg-blue-500 transition-colors"
              title="Refresh dashboard"
            >
              <RefreshCw className="w-5 h-5" />
            </button>
          </div>
          <p className="mt-1 opacity-80">Upload and analyze financial SMS messages</p>
        </div>
        
        <div className="p-6">
          <div className="mb-6 border rounded-lg overflow-hidden">
            <div className="bg-gray-100 p-4 border-b">
              <h3 className="text-lg font-medium">Upload Messages</h3>
            </div>
            <div className="p-4">
              <div className="mb-5">
                <div className="flex items-center space-x-4">
                  <div className="flex-1">
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Select CSV File
                    </label>
                    <div className="mt-1 flex justify-center px-6 pt-5 pb-6 border-2 border-gray-300 border-dashed rounded-md">
                      <div className="space-y-1 text-center">
                        <Upload className="mx-auto h-12 w-12 text-gray-400" />
                        <div className="flex text-sm text-gray-600">
                          <label
                            htmlFor="file-upload"
                            className="relative cursor-pointer bg-white rounded-md font-medium text-blue-600 hover:text-blue-500"
                          >
                            <span>Upload a file</span>
                            <input
                              id="file-upload"
                              name="file-upload"
                              type="file"
                              className="sr-only"
                              accept=".csv"
                              onChange={handleFileChange}
                            />
                          </label>
                          <p className="pl-1">or drag and drop</p>
                        </div>
                        <p className="text-xs text-gray-500">CSV files only</p>
                      </div>
                    </div>
                    {selectedFile && (
                      <p className="mt-2 text-sm text-gray-500">
                        Selected file: {selectedFile.name}
                      </p>
                    )}
                  </div>
                  <div className="flex flex-col space-y-2">
                    <button
                      onClick={handleFileUpload}
                      className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
                      disabled={!selectedFile || processing}
                    >
                      <Upload className="w-4 h-4 mr-2" />
                      Upload & Process
                    </button>
                    <button
                      onClick={handleProcessExistingFile}
                      className="inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md shadow-sm text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
                      disabled={processing}
                    >
                      <FilePlus className="w-4 h-4 mr-2" />
                      Process Existing File
                    </button>
                  </div>
                </div>
              </div>
              
              {uploadResponse && (
                <div className={`p-4 rounded mb-4 ${uploadResponse.status === 'success' ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'}`}>
                  <div className="flex items-start">
                    {uploadResponse.status === 'success' ? (
                      <Check className="w-5 h-5 text-green-500 mr-2 mt-0.5" />
                    ) : (
                      <X className="w-5 h-5 text-red-500 mr-2 mt-0.5" />
                    )}
                    <p className={uploadResponse.status === 'success' ? 'text-green-700' : 'text-red-700'}>
                      {uploadResponse.message}
                    </p>
                  </div>
                </div>
              )}
              
              {processing && renderProgressBar()}
            </div>
          </div>
          
          {renderSummaryStats()}
          {renderRecentMessages()}
          {renderRecentErrors()}
        </div>
      </div>
    </div>
  );
};

export default FinancialDashboard;