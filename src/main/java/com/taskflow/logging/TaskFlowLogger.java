package com.taskflow.logging;

import java.util.logging.Logger;

/**
 * Object-oriented logger for TaskFlow framework.
 * Provides centralized logging with configurable verbosity levels.
 */
public class TaskFlowLogger {
    private final Logger delegate;
    private final boolean verbose;
    private final boolean printEnabled;
    
    /**
     * Creates a TaskFlowLogger with specified configuration.
     * 
     * @param delegate the underlying Java Logger
     * @param verbose whether to enable verbose (FINE level) logging
     * @param printEnabled whether to enable any logging output
     */
    public TaskFlowLogger(Logger delegate, boolean verbose, boolean printEnabled) {
        this.delegate = delegate;
        this.verbose = verbose;
        this.printEnabled = printEnabled;
    }
    
    /**
     * Creates a TaskFlowLogger with default configuration (verbose=false, printEnabled=true).
     * 
     * @param delegate the underlying Java Logger
     */
    public TaskFlowLogger(Logger delegate) {
        this(delegate, false, true);
    }
    
    /**
     * Creates a TaskFlowLogger for a specific class.
     * 
     * @param clazz the class for which to create the logger
     * @param verbose whether to enable verbose logging
     * @param printEnabled whether to enable logging output
     * @return a new TaskFlowLogger instance
     */
    public static TaskFlowLogger forClass(Class<?> clazz, boolean verbose, boolean printEnabled) {
        return new TaskFlowLogger(Logger.getLogger(clazz.getName()), verbose, printEnabled);
    }
    
    /**
     * Creates a TaskFlowLogger for a specific class with default configuration.
     * 
     * @param clazz the class for which to create the logger
     * @return a new TaskFlowLogger instance
     */
    public static TaskFlowLogger forClass(Class<?> clazz) {
        return forClass(clazz, false, true);
    }
    
    /**
     * Logs an informational message.
     * 
     * @param message the message to log
     */
    public void info(String message) {
        if (printEnabled) {
            delegate.info(message);
        }
    }
    
    /**
     * Logs an informational message with formatting.
     * 
     * @param format the format string
     * @param args the arguments for formatting
     */
    public void info(String format, Object... args) {
        if (printEnabled) {
            delegate.info(String.format(format, args));
        }
    }
    
    /**
     * Logs a warning message.
     * 
     * @param message the message to log
     */
    public void warning(String message) {
        if (printEnabled) {
            delegate.warning(message);
        }
    }
    
    /**
     * Logs a warning message with formatting.
     * 
     * @param format the format string
     * @param args the arguments for formatting
     */
    public void warning(String format, Object... args) {
        if (printEnabled) {
            delegate.warning(String.format(format, args));
        }
    }
    
    /**
     * Logs a severe/error message.
     * 
     * @param message the message to log
     */
    public void severe(String message) {
        if (printEnabled) {
            delegate.severe(message);
        }
    }
    
    /**
     * Logs a severe/error message with formatting.
     * 
     * @param format the format string
     * @param args the arguments for formatting
     */
    public void severe(String format, Object... args) {
        if (printEnabled) {
            delegate.severe(String.format(format, args));
        }
    }
    
    /**
     * Logs a fine/verbose message (only if verbose mode is enabled).
     * 
     * @param message the message to log
     */
    public void fine(String message) {
        if (printEnabled && verbose) {
            delegate.fine(message);
        }
    }
    
    /**
     * Logs a fine/verbose message with formatting (only if verbose mode is enabled).
     * 
     * @param format the format string
     * @param args the arguments for formatting
     */
    public void fine(String format, Object... args) {
        if (printEnabled && verbose) {
            delegate.fine(String.format(format, args));
        }
    }
    
    /**
     * Checks if verbose logging is enabled.
     * 
     * @return true if verbose mode is enabled
     */
    public boolean isVerbose() {
        return verbose;
    }
    
    /**
     * Checks if logging output is enabled.
     * 
     * @return true if print is enabled
     */
    public boolean isPrintEnabled() {
        return printEnabled;
    }
}
