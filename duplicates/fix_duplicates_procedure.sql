create or replace PROCEDURE FIX_DUPLICADOS_PEDIDOS(p_delete_duplicates IN BOOLEAN DEFAULT FALSE) IS
  v_count_deleted NUMBER := 0;
  v_audit_count   NUMBER := 0;
  v_msg           VARCHAR2(4000);
BEGIN
  -- 1) Insertar duplicados en tabla de auditoría
  INSERT INTO AUDIT_DUP_PEDIDOS
  SELECT p.ID_PEDIDO, p.ID_CLIENTE, p.PRODUCTO, p.FECHA_COMPRA
  FROM (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY ID_CLIENTE, PRODUCTO ORDER BY FECHA_COMPRA DESC, ROWID) rn
    FROM PEDIDOS p
  ) p
  WHERE p.rn > 1;

  -- 2) (Opcional) eliminar duplicados manteniendo la fila con rn = 1
  IF p_delete_duplicates THEN
    DELETE FROM PEDIDOS
    WHERE ROWID IN (
      SELECT rid FROM (
        SELECT ROWID AS rid,
               ROW_NUMBER() OVER (PARTITION BY ID_CLIENTE, PRODUCTO ORDER BY FECHA_COMPRA DESC, ROWID) rn
        FROM PEDIDOS
      ) WHERE rn > 1
    );

    v_count_deleted := SQL%ROWCOUNT;
  END IF;

  COMMIT;

  -- 3) obtener cuenta de registros auditados y construir mensaje de salida
  SELECT COUNT(*) INTO v_audit_count FROM AUDIT_DUP_PEDIDOS;

  v_msg := ‘Duplicados auditados: ‘ || v_audit_count;
  IF p_delete_duplicates THEN
    v_msg := v_msg || ‘ ; Eliminados: ‘ || v_count_deleted;
  END IF;

  DBMS_OUTPUT.PUT_LINE(v_msg);

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE(’Error en FIX_DUPLICADOS_PEDIDOS: ‘ || SQLERRM);
    RAISE;
END FIX_DUPLICADOS_PEDIDOS;
/
