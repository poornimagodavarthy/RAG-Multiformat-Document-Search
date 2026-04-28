import React, { useState, useEffect } from "react";
import { Search, FileText, Download, X, ChevronRight, Upload, Eye} from "lucide-react";

const RAGSearch = () => {
  const [query, setQuery] = useState("");
  const [isSearching, setIsSearching] = useState(false);
  const [results, setResults] = useState(null);
  const [showUpload, setShowUpload] = useState(false);
  const [showDocuments, setShowDocuments] = useState(false);
  const [viewingDocument, setViewingDocument] = useState(null);
  const [documentLibrary, setDocumentLibrary] = useState([]);
  const [isLoadingDocs, setIsLoadingDocs] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(null);
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [isDragging, setIsDragging] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState(null);
  const [showLanding, setShowLanding] = useState(true);


  const API_BASE_URL = "https://ragenginebackend.fly.dev";

  const uploadDocument = async (file) => {
    const formData = new FormData();
    formData.append('file', file);
  
    const response = await fetch(`${API_BASE_URL}/upload`, {
      method: 'POST',
      body: formData,
    });
  
    if (!response.ok) {
      throw new Error('Upload failed');
    }
  
    // Backend returns empty / non-JSON response, don’t parse
    return { success: true };
  };
  

  const searchKnowledgeBase = async (query) => {
    const response = await fetch(`${API_BASE_URL}/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });
    if (!response.ok) throw new Error(`Search failed: ${response.statusText}`);
    return response.json();
  };

  const getAllDocuments = async () => {
    const response = await fetch(`${API_BASE_URL}/documents`);
    if (!response.ok) throw new Error("Failed to fetch documents");
    return response.json();
  };

  useEffect(() => {
    const fetchDocuments = async () => {
      setIsLoadingDocs(true);
      try {
        const data = await getAllDocuments();
        setDocumentLibrary(data.documents);
      } catch (error) {
        console.error("Error loading documents:", error);
      } finally {
        setIsLoadingDocs(false);
      }
    };

    fetchDocuments();
    
  }, []);

  const handleFileUpload = async (event) => {
    const files = Array.from(event.target.files);
  
    for (const file of files) {
      try {
        setUploadProgress(`Uploading ${file.name}...`);
  
        const result = await uploadDocument(file);
  
        if (result.success) {
          setUploadedFiles(prev => [...prev, file.name]);
          setUploadProgress(`${file.name} uploaded, processing (this may take 30-60 seconds)`);
        } else {
          setUploadProgress(`${file.name} upload failed`);
        }
  
      } catch (error) {
        console.error("Upload error:", error);
        setUploadProgress(`${file.name} upload failed: ${error.message}`);
      }
    }
  
    // Poll for updated documents every 5 seconds for up to 2 minutes
    setUploadProgress("Processing documents... checking for updates");
    let attempts = 0;
    const maxAttempts = 24; 
    
    const pollInterval = setInterval(async () => {
      attempts++;
      try {
        const data = await getAllDocuments();
        setDocumentLibrary(data.documents);
        
        // Check if all uploaded files appear in the library
        const allProcessed = uploadedFiles.every(filename => 
          data.documents.some(doc => doc.filename === filename)
        );
        
        if (allProcessed || attempts >= maxAttempts) {
          clearInterval(pollInterval);
          setUploadProgress(allProcessed 
            ? "All documents processed and ready!" 
            : "Documents uploaded but still processing. Refresh to see updates.");
        }
      } catch (error) {
        console.error("Error checking documents:", error);
      }
    }, 5000); // Check every 5 seconds
  };
  

  const handleDeleteDocument = async (docId) => {
    try {
      await fetch(`${API_BASE_URL}/documents/${docId}`, { method: 'DELETE' });
      const data = await getAllDocuments();
      setDocumentLibrary(data.documents);
      setDeleteConfirm(null);
    } catch (error) {
      console.error('Delete error:', error);
    }
  };

  const handleDragEnter = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = async (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  
    const files = Array.from(e.dataTransfer.files);
  
    for (const file of files) {
      try {
        setUploadProgress(`Uploading ${file.name}...`);
  
        const result = await uploadDocument(file);
  
        if (result.success) {
          setUploadedFiles(prev => [...prev, file.name]);
          setUploadProgress(`${file.name} uploaded, processing`);
        } else {
          setUploadProgress(`${file.name} upload failed`);
        }
  
      } catch (error) {
        console.error("Upload error:", error);
        setUploadProgress(`${file.name} upload failed`);
      }
    }
  };
  

  const handleSearch = async () => {
    if (!query.trim()) return;
    setIsSearching(true);
    setResults(null);
    try {
      const data = await searchKnowledgeBase(query);
      // Group sources by title
      const groupedSources = data.sources.reduce((acc, source) => {
        const key = source.title;
        if (!acc[key]) {
          acc[key] = {
            id: source.id,
            title: source.title,
            type: source.type,
            date: `Updated ${source.date}`,
            s3Url: source.s3_url,
            pages: [],
            excerpts: []
          };
        }
        acc[key].pages.push(source.page);
        acc[key].excerpts.push(source.excerpt);
        return acc;
      }, {});

setResults({
  answer: data.answer,
  sources: Object.values(groupedSources)
});
    } catch (error) {
      console.error("Search error:", error);
      setResults({
        answer: "Sorry, there was an error searching the knowledge base. Please try again.",
        sources: []
      });
    } finally {
      setIsSearching(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === "Enter") {
      handleSearch();
    }
  };

  const handleViewDocument = (source) => {
    const pdfUrl = `${source.s3Url}#page=${source.page}`;
    setViewingDocument({ ...source, pdfUrl });
  };

  const handleDownload = (s3Url, filename) => {
    window.open(s3Url, '_blank');
  };
  if (showLanding) {
    return (
      <div className="min-h-screen bg-neutral-50 flex items-center justify-center p-6">
        <div className="bg-white rounded-xl border border-neutral-200 p-8 shadow-sm max-w-md w-full text-center">
          <div className="w-12 h-12 bg-red-900 rounded-lg flex items-center justify-center mx-auto mb-4">
            <span className="text-white font-serif text-lg">P</span>
          </div>
          <h1 className="text-2xl font-serif text-neutral-900 mb-1">RAG Search Engine</h1>
          <p className="text-sm text-neutral-500 mb-6">Multiformat Document Intelligence</p>
          <p className="text-neutral-600 mb-6">
            Hi! Thanks for checking out my demo project. This is a multi format semantic search system 
            that lets you upload any document and ask questions - powered by RAG with OpenAI vector embeddings.
          </p>
          <div className="text-left bg-neutral-50 rounded-lg p-4 mb-6">
            <p className="text-xs font-medium text-neutral-500 uppercase tracking-widest mb-2">Stack</p>
            <p className="text-sm text-neutral-600">FastAPI · Qdrant · OpenAI Embeddings · Redis · AWS S3 · React</p>
          </div>
          <button
            onClick={() => setShowLanding(false)}
            className="w-full bg-red-900 text-white py-3 rounded-lg hover:bg-red-950 transition-colors font-medium"
          >
            View Demo →
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-neutral-50">
      <div className="bg-red-900 text-white text-center text-xs py-1.5 tracking-widest font-medium uppercase">
        Demo Only
      </div>
      <header className="bg-white border-b border-neutral-200">
        <div className="max-w-6xl mx-auto px-6 py-6 flex items-center justify-between">
          <div className="flex items-center gap-3 cursor-pointer" onClick={() => {
            setShowDocuments(false);
            setShowUpload(false);
          }}>
            <div className="w-10 h-10 bg-red-900 rounded-lg flex items-center justify-center">
              <span className="text-white font-serif text-lg">P</span>
            </div>
            <div>
              <h1 className="text-xl font-serif text-neutral-900">RAG Search Engine</h1>
              <p className="text-xs text-neutral-500">Multiformat Document Intelligence</p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <button
              onClick={() => {
                setShowDocuments(!showDocuments);
                setShowUpload(false);
              }}
              className="flex items-center gap-2 px-4 py-2 text-neutral-700 hover:text-red-900 transition-colors"
            >
              <FileText className="w-4 h-4" />
              <span className="text-sm">Documents</span>
            </button>
            <button
              onClick={() => {
                setShowUpload(!showUpload);
                setShowDocuments(false);
              }}
              className="flex items-center gap-2 px-4 py-2 text-neutral-700 hover:text-red-900 transition-colors"
            >
              <Upload className="w-4 h-4" />
              <span className="text-sm">Upload</span>
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-6 py-12">
        {showDocuments && (
          <div className="bg-white rounded-xl border border-neutral-200 p-8 shadow-sm mb-8">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h3 className="text-lg font-serif text-neutral-900 mb-1">Document Library</h3>
                <p className="text-sm text-neutral-500">{documentLibrary.length} documents available</p>
              </div>
              <button onClick={() => setShowDocuments(false)} className="text-neutral-400 hover:text-neutral-900 transition-colors">
                <X className="w-5 h-5" />
              </button>
            </div>

            {isLoadingDocs ? (
              <div className="text-center py-12">
                <div className="w-12 h-12 border-4 border-neutral-200 border-t-red-900 rounded-full animate-spin mx-auto mb-4"></div>
                <p className="text-neutral-600">Loading documents...</p>
              </div>
            ) : documentLibrary.length === 0 ? (
              <div className="text-center py-12">
                <FileText className="w-12 h-12 text-neutral-300 mx-auto mb-4" />
                <p className="text-neutral-600">No documents found</p>
              </div>
            ) : (
              <div className="grid gap-4">
                {documentLibrary.map((doc) => (
                  <div key={doc.id} className="border border-neutral-200 rounded-lg p-5 hover:border-red-900 transition-all group">
                    <div className="flex items-start justify-between">
                      <div className="flex items-start gap-4 flex-1">
                        <div className="w-12 h-12 bg-red-900/10 rounded-lg flex items-center justify-center flex-shrink-0">
                          <FileText className="w-6 h-6 text-red-900" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <h4 className="font-medium text-neutral-900 group-hover:text-red-900 transition-colors mb-2">{doc.name}</h4>
                          <div className="flex flex-wrap items-center gap-3 mb-3">
                            <span className="px-2.5 py-1 bg-neutral-100 text-neutral-700 text-xs rounded-full">{doc.type}</span>
                            <span className="text-xs text-neutral-500">{doc.pages} pages</span>
                            <span className="text-xs text-neutral-400">•</span>
                            <span className="text-xs text-neutral-500">{doc.size}</span>
                            <span className="text-xs text-neutral-400">•</span>
                            <span className="text-xs text-neutral-500">{doc.date}</span>
                          </div>
                          <div className="flex items-center gap-3">
                            <button
                              onClick={() => handleViewDocument({ ...doc, page: 1, s3Url: doc.s3_url })}
                              className="flex items-center gap-2 px-4 py-2 bg-red-900 text-white text-sm rounded-lg hover:bg-red-950 transition-colors"
                            >
                              <Eye className="w-4 h-4" />
                              View
                            </button>
                            <button
                              onClick={() => handleDownload(doc.s3_url, doc.name)}
                              className="flex items-center gap-2 px-4 py-2 border border-neutral-300 text-neutral-700 text-sm rounded-lg hover:border-red-900 hover:text-red-900 transition-colors"
                            >
                              <Download className="w-4 h-4" />
                              Download
                            </button>
                            <button
                              onClick={() => setDeleteConfirm(doc)}
                              className="flex items-center gap-2 px-4 py-2 border border-neutral-300 text-neutral-700 text-sm rounded-lg hover:border-red-900 hover:text-red-900 transition-colors"
                            >
                              <X className="w-4 h-4" />
                              Delete
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}

            <div className="mt-6 pt-6 border-t border-neutral-200">
              <div className="grid grid-cols-4 gap-4 text-center">
                <div>
                  <p className="text-2xl font-serif text-red-900 mb-1">{documentLibrary.length}</p>
                  <p className="text-xs text-neutral-500">Total Documents</p>
                </div>
              </div>
            </div>
          </div>
        )}

        {deleteConfirm && (
          <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center p-6 z-50">
            <div className="bg-white rounded-xl max-w-md w-full p-6 shadow-2xl">
              <h3 className="text-lg font-serif text-neutral-900 mb-2">Delete Document?</h3>
              <p className="text-neutral-600 mb-6">
                Are you sure you want to delete "{deleteConfirm.name}"? This action cannot be undone.
              </p>
              <div className="flex items-center gap-3 justify-end">
                <button onClick={() => setDeleteConfirm(null)} className="px-4 py-2 text-neutral-700 hover:text-neutral-900 transition-colors">
                  Cancel
                </button>
                <button
                  onClick={() => handleDeleteDocument(deleteConfirm.document_id)}
                  className="px-4 py-2 bg-red-900 text-white rounded-lg hover:bg-red-950 transition-colors"
                >
                  Delete
                </button>
              </div>
            </div>
          </div>
        )}

        {showUpload && (
          <div className="bg-white rounded-xl border border-neutral-200 p-8 shadow-sm mb-8">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-lg font-serif text-neutral-900">Document Upload</h3>
              <button onClick={() => setShowUpload(false)} className="text-neutral-400 hover:text-neutral-900 transition-colors">
                <X className="w-5 h-5" />
              </button>
            </div>
            <div 
              className={`border-2 border-dashed rounded-lg p-12 text-center transition-colors ${
                isDragging ? 'border-red-900 bg-red-50' : 'border-neutral-300 hover:border-red-900'
              }`}
              onDragEnter={handleDragEnter}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
            >
              <input
                type="file"
                id="file-upload"
                className="hidden"
                onChange={handleFileUpload}
                multiple
                accept=".pdf,.docx,.doc,.md,.txt,.csv,.xlsx,.xls"
              />
              <label htmlFor="file-upload" className="cursor-pointer">
                <Upload className="w-12 h-12 text-neutral-400 mx-auto mb-4" />
                <p className="text-neutral-700 mb-2">Drag and drop files here</p>
                <p className="text-sm text-neutral-500 mb-4">Supports PDF, Markdown, CSV, Word documents</p>
                <span className="px-6 py-2.5 bg-red-900 text-white rounded-lg hover:bg-red-950 transition-colors inline-block">
                  Browse Files
                </span>
              </label>
              {uploadProgress && <p className="mt-4 text-sm text-neutral-600">{uploadProgress}</p>}
            </div>
            <div className="mt-6">
              <div className="flex items-center justify-between mb-3">
                <p className="text-sm font-medium text-neutral-900">Recently Uploaded ({uploadedFiles.length})</p>
              </div>
              {uploadedFiles.length > 0 ? (
                <div className="space-y-2">
                  {uploadedFiles.map((filename, idx) => (
                    <div key={idx} className="flex items-center justify-between p-3 bg-neutral-50 rounded-lg">
                      <div className="flex items-center gap-3">
                        <FileText className="w-4 h-4 text-red-900" />
                        <span className="text-sm text-neutral-700">{filename}</span>
                      </div>
                      <span className="text-xs text-green-600">✓ Processed</span>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-neutral-500 text-center py-4">No files uploaded yet</p>
              )}
            </div>
          </div>
        )}

        <div className="mb-12">
          <h2 className="text-3xl font-serif text-neutral-900 mb-3">Multiformat Document Search & Retrieval</h2>
          <p className="text-neutral-600 mb-4">Upload any document and ask questions - powered by RAG with semantic search</p>
          <p className="text-neutral-500 mb-6">(Supports PDF, CSV, Markdown, Text, Excel and Word Docs)</p>

          <div className="mb-4 flex flex-wrap gap-2">
            {[
              "What are the key findings in this document?",
              "Summarize the main topics covered",
              "What are the action items mentioned?",
              "Find references to budget or costs",
              "What decisions were made?",
              "Who are the stakeholders mentioned?"
            ].map((suggestion, idx) => (
              <button
                key={idx}
                onClick={() => setQuery(suggestion)}
                className="px-3 py-1.5 text-sm bg-white border border-neutral-200 rounded-full hover:border-red-900 hover:text-red-900 transition-colors"
              >
                {suggestion}
              </button>
            ))}
          </div>

          <div className="relative">
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyPress={handleKeyPress}
              placeholder="e.g., What are the key findings in this document?"
              className="w-full px-6 py-5 pr-14 rounded-lg border border-neutral-300 focus:border-red-900 focus:ring-2 focus:ring-red-900/10 outline-none transition-all text-neutral-900 placeholder:text-neutral-400"
            />
            <button
              onClick={handleSearch}
              disabled={isSearching || !query.trim()}
              className="absolute right-3 top-1/2 -translate-y-1/2 bg-red-900 text-white p-3 rounded-md hover:bg-red-950 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
            >
              <Search className="w-5 h-5" />
            </button>
          </div>
        </div>

        {isSearching && (
          <div className="flex flex-col items-center justify-center py-20">
            <div className="w-16 h-16 border-4 border-neutral-200 border-t-red-900 rounded-full animate-spin mb-4"></div>
            <p className="text-neutral-600">Searching knowledge base...</p>
          </div>
        )}

        {results && !isSearching && (
          <div className="space-y-8">
            <div className="bg-white rounded-xl border border-neutral-200 p-8 shadow-sm">
              <div className="flex items-start gap-3 mb-4">
                <div className="w-8 h-8 bg-red-900 rounded-lg flex items-center justify-center flex-shrink-0 mt-1">
                  <ChevronRight className="w-5 h-5 text-white" />
                </div>
                <div className="flex-1">
                  <h3 className="text-lg font-serif text-neutral-900 mb-3">Answer</h3>
                  <p className="text-neutral-700 leading-relaxed">{results.answer}</p>
                </div>
              </div>
            </div>

            <div>
              <h3 className="text-lg font-serif text-neutral-900 mb-4">Sources ({results.sources.length})</h3>
              <div className="grid gap-4">
              {results.sources.map((source) => (
                  <div key={source.id} className="bg-white rounded-lg border border-neutral-200 p-6 hover:border-red-900 transition-all group">
                    <div className="flex items-start justify-between mb-3">
                      <div className="flex items-start gap-3 flex-1">
                        <FileText className="w-5 h-5 text-red-900 flex-shrink-0 mt-0.5" />
                        <div className="flex-1">
                          <h4 className="font-medium text-neutral-900 group-hover:text-red-900 transition-colors mb-1">{source.title}</h4>
                          <div className="flex items-center gap-3 mb-3">
                            <span className="text-xs text-neutral-500">{source.type}</span>
                            <span className="text-xs text-neutral-400">•</span>
                            <span className="text-xs text-neutral-500">{source.date}</span>
                            <span className="text-xs text-neutral-400">•</span>
                            <span className="text-xs font-medium text-red-900">{source.pages.length} relevant {source.pages.length === 1 ? 'page' : 'pages'}</span>
                          </div>
                          <p className="text-sm text-neutral-600 leading-relaxed mb-4">
                            {source.excerpts[0]}
                          </p>
                          <div className="flex items-center gap-3 flex-wrap">
                            {source.pages.map((page, idx) => (
                              <button
                                key={idx}
                                onClick={() => handleViewDocument({...source, page})}
                                className="flex items-center gap-2 px-4 py-2 bg-red-900 text-white text-sm rounded-lg hover:bg-red-950 transition-colors"
                              >
                                <Eye className="w-4 h-4" />
                                View Page {page}
                              </button>
                            ))}
                            <button
                              onClick={() => handleDownload(source.s3Url, source.title)}
                              className="flex items-center gap-2 px-4 py-2 border border-neutral-300 text-neutral-700 text-sm rounded-lg hover:border-red-900 hover:text-red-900 transition-colors"
                            >
                              <Download className="w-4 h-4" />
                              Download
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
                
              </div>
            </div>
          </div>
        )}

        {!results && !isSearching && (
          <div className="text-center py-20">
            <div className="w-16 h-16 bg-neutral-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <Search className="w-8 h-8 text-neutral-400" />
            </div>
            <h3 className="text-lg font-serif text-neutral-900 mb-2">Start Your Search</h3>
            <p className="text-neutral-600">Enter a question to search across your uploaded documents</p>
          </div>
        )}
      </main>

      {viewingDocument && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center p-6 z-50">
          <div className="bg-white rounded-xl max-w-5xl w-full h-[85vh] overflow-hidden shadow-2xl flex flex-col">
            <div className="flex items-start justify-between p-6 border-b border-neutral-200">
              <div className="flex-1">
                <h3 className="text-xl font-serif text-neutral-900 mb-2">{viewingDocument.title}</h3>
                <div className="flex items-center gap-3">
                  <span className="text-sm text-neutral-500">{viewingDocument.type}</span>
                  <span className="text-neutral-300">•</span>
                  <span className="text-sm text-neutral-500">{viewingDocument.date}</span>
                  <span className="text-neutral-300">•</span>
                  <span className="text-sm font-medium text-red-900">Page {viewingDocument.page}</span>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <button
                  onClick={() => handleDownload(viewingDocument.s3Url, viewingDocument.title)}
                  className="px-4 py-2 border border-neutral-300 text-neutral-700 rounded-lg hover:border-red-900 hover:text-red-900 transition-colors flex items-center gap-2"
                >
                  <Download className="w-4 h-4" />
                  Download
                </button>
                <button onClick={() => setViewingDocument(null)} className="text-neutral-400 hover:text-neutral-900 transition-colors">
                  <X className="w-6 h-6" />
                </button>
              </div>
            </div>
            <div className="flex-1 overflow-auto bg-neutral-100">
              <iframe src={viewingDocument.pdfUrl} className="w-full h-full border-0" title={viewingDocument.title} style={{ minHeight: '100%' }} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default RAGSearch;