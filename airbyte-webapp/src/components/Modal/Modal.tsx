import React, { useEffect, useCallback } from "react";
import { createPortal } from "react-dom";

import ContentCard from "components/ContentCard";

import styles from "./Modal.module.scss";

export interface ModalProps {
  title?: string | React.ReactNode;
  onClose?: () => void;
  clear?: boolean;
  closeOnBackground?: boolean;
}

const Modal: React.FC<ModalProps> = ({ children, title, onClose, clear, closeOnBackground }) => {
  const handleUserKeyPress = useCallback((event, closeModal) => {
    const { keyCode } = event;
    if (keyCode === 27) {
      closeModal();
    }
  }, []);

  useEffect(() => {
    onClose && window.addEventListener("keydown", (event) => handleUserKeyPress(event, onClose));

    return () => {
      onClose && window.removeEventListener("keydown", (event) => handleUserKeyPress(event, onClose));
    };
  }, [handleUserKeyPress, onClose]);

  return createPortal(
    <div className={styles.modal} onClick={() => (closeOnBackground && onClose ? onClose() : null)}>
      {clear ? (
        children
      ) : (
        <ContentCard title={title} className={styles.card}>
          {children}
        </ContentCard>
      )}
    </div>,
    document.body
  );
};

export default Modal;
