package apoc.load;


import com.novell.ldap.LDAPEntry;
import com.novell.ldap.LDAPSearchResults;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LoadLdapTest {

    @Test
    public void testLoadLDAP() throws Exception {
        Map<String, Object> connParams = new HashMap<>();
        connParams.put("ldapHost", "ldap.forumsys.com");
        connParams.put("ldapPort", 389l);
        connParams.put("loginDN", "cn=read-only-admin,dc=example,dc=com");
        connParams.put("loginPW", "password");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(LoadLdap.getConnectionMap(connParams));
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("searchBase", "dc=example,dc=com");
        searchParams.put("searchScope", "SCOPE_ONE");
        searchParams.put("searchFilter", "(&(objectClass=*)(uid=training))");
        ArrayList<String> ats = new ArrayList<>();
        ats.add("uid");
        searchParams.put("attributes", ats);
        LDAPSearchResults results = mgr.doSearch(searchParams);
        LDAPEntry le = results.next();
        assertEquals("uid=training,dc=example,dc=com", le.getDN());
        assertEquals("training", le.getAttribute("uid").getStringValue());
    }

}

