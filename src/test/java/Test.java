import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dooioo.zkjob.MainClient;


@Component
public class Test {
	public static void main(String[] args) {
		@SuppressWarnings("unused")
		MainClient c = new MainClient();
		String l = "/locks/ems/com.dooioo.salary.jobs.CommissionJob.public void com.dooioo.salary.jobs.CommissionJob.updateCommissionToSalary()";
		c.zk.delete(l);
	}
	@Scheduled(cron="0 0/1 * * * ?")
	public void testf(){
		System.out.println("terst go");
	}
}
