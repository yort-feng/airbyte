import styles from "./Modal.module.scss";

interface ModalBodyProps {
  maxWidth: number | string;
  maxHeight?: number | string;
}

export const ModalBody: React.FC<ModalBodyProps> = ({ children, maxHeight, maxWidth }) => {
  return (
    <div className={styles.body} style={{ maxHeight, maxWidth }}>
      {children}
    </div>
  );
};
