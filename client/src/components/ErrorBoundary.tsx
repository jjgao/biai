import { Component, ErrorInfo, ReactNode } from 'react'

interface Props {
    children: ReactNode
}

interface State {
    hasError: boolean
    error: Error | null
}

class ErrorBoundary extends Component<Props, State> {
    constructor(props: Props) {
        super(props)
        this.state = {
            hasError: false,
            error: null
        }
    }

    static getDerivedStateFromError(error: Error): State {
        return {
            hasError: true,
            error
        }
    }

    componentDidCatch(error: Error, errorInfo: ErrorInfo) {
        // Log error to console - could be sent to tracking service here
        console.error('ErrorBoundary caught error:', error, errorInfo)
    }

    handleReload = () => {
        window.location.reload()
    }

    render() {
        if (this.state.hasError) {
            return (
                <div style={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    height: '100vh',
                    backgroundColor: '#f5f5f5',
                    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
                }}>
                    <div style={{
                        backgroundColor: 'white',
                        padding: '2rem',
                        borderRadius: '8px',
                        boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
                        maxWidth: '500px',
                        width: '90%',
                        textAlign: 'center'
                    }}>
                        <h2 style={{
                            color: '#d32f2f',
                            marginBottom: '1rem',
                            marginTop: 0
                        }}>
                            Something went wrong
                        </h2>

                        <p style={{
                            color: '#555',
                            marginBottom: '1.5rem',
                            lineHeight: '1.5'
                        }}>
                            The application encountered an unexpected error.
                        </p>

                        {this.state.error && (
                            <pre style={{
                                backgroundColor: '#f8d7da',
                                color: '#721c24',
                                padding: '1rem',
                                borderRadius: '4px',
                                fontSize: '0.85rem',
                                overflow: 'auto',
                                marginBottom: '1.5rem',
                                textAlign: 'left',
                                maxHeight: '200px'
                            }}>
                                {this.state.error.toString()}
                            </pre>
                        )}

                        <button
                            onClick={this.handleReload}
                            style={{
                                backgroundColor: '#1976d2',
                                color: 'white',
                                border: 'none',
                                padding: '10px 20px',
                                borderRadius: '4px',
                                fontSize: '1rem',
                                cursor: 'pointer',
                                transition: 'background-color 0.2s'
                            }}
                        >
                            Reload Page
                        </button>
                    </div>
                </div>
            )
        }

        return this.props.children
    }
}

export default ErrorBoundary
