package com.learn.analyze.dao.factory;

import com.learn.analyze.dao.ISessionDetailDAO;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDAOTest {
	
	public static void main(String[] args) {
		ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
		// boolean exist = sessionDetailDAO.isExist("7f58d8782d7540fdaacf0c63c68af2d0");
		// System.out.println(exist);
	}
	
}
