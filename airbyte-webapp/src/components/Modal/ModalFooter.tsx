import styles from "./Modal.module.scss";

export const ModalFooter: React.FC = ({ children }) => {
  return <div className={styles.footer}>{children}</div>;
};
