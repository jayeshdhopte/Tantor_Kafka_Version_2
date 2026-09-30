import React from "react";
import { createPortal } from "react-dom";
import "./NewClusterModal.css";

interface Props {
  onClose: () => void;
  onCreate: () => void;
  onExplore: () => void;
}

export const NewClusterModal: React.FC<Props> = ({ onClose, onCreate, onExplore }) => {
  return createPortal(
    <div className="cd-modal-overlay" onClick={onClose}>
      <div className="cd-deployment-modal" onClick={e => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="cd-deployment-modal-header">
          <div className="cd-deployment-modal-header-content">
            <h2>Cluster Development</h2>
            <p>Create a managed Kafka cluster or connect an existing external cluster.</p>
          </div>
          <button className="cd-icon-btn close-btn" onClick={onClose} title="Close">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6L6 18M6 6l12 12"/></svg>
          </button>
        </div>
        <div className="cd-deployment-cards-wrapper">
          <div className="cd-deployment-choice-grid">
            <div className="cd-deployment-card" onClick={onCreate}>
              <div className="cd-deployment-card-content">
                <svg className="cluster-choice-icon managed" width="24" height="24" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                  <circle cx="12" cy="4.5" r="3.25" />
                  <path d="M12 7.75v6M5 13.75h14M5 13.75V17M19 13.75V17" fill="none" stroke="currentColor" strokeWidth="2.5" />
                  <rect x="2" y="17" width="6" height="5" rx="0.5" />
                  <rect x="16" y="17" width="6" height="5" rx="0.5" />
                </svg>
                <h3>Create your Cluster</h3>
                <p>Build a new KRaft or ZooKeeper cluster on selected Tantor host</p>
              </div>
              <button className="cd-deployment-btn outline" onClick={onCreate}>Create</button>
            </div>
            <div className="cd-deployment-card" onClick={onExplore}>
              <div className="cd-deployment-card-content">
                <svg className="cluster-choice-icon existing" width="24" height="24" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M22 11V3h-7v3H9V3H2v8h7V8h2v10h4v3h7v-8h-7v3h-2V8h2v3h7v-8zM7 9H4V5h3v4zm13-4h-3V5h3v4zm0 14h-3v-4h3v4z"/></svg>
                <h3>Existing Cluster</h3>
                <p>Connect or discover an external Kafka cluster</p>
              </div>
              <button className="cd-deployment-btn outline" onClick={onExplore}>Explorer</button>
            </div>
          </div>
        </div>
      </div>
    </div>,
    document.body
  );
};
