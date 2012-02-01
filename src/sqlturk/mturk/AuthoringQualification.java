/*
 * Please note that the qualification codes are probably different in
 * in different mode (production mode or sandbox mode.) Please refer
 * to http://docs.amazonwebservices.com/AWSMechTurk/latest/AWSMturkAPI/ApiReference_QualificationRequirementDataStructureArticle.html
 * for more information.
 */

package sqlturk.mturk;

import sqlturk.configuration.Parameters;

import com.amazonaws.mturk.requester.Comparator;
//import com.amazonaws.mturk.requester.Locale;
import com.amazonaws.mturk.requester.QualificationRequirement;
import com.amazonaws.mturk.service.axis.RequesterService;

// creates qualification requirements for our app
class AuthoringQualification {

    private AuthoringQualification() {
	throw new AssertionError();
    }

    public static QualificationRequirement[] createQualReqs() {

	// QualificationRequirement qualReq = new QualificationRequirement();
	// qualReq.setQualificationTypeId(RequesterService.APPROVAL_RATE_QUALIFICATION_TYPE_ID);
	// qualReq.setComparator(Comparator.GreaterThanOrEqualTo);
	// qualReq.setIntegerValue(Parameters.MIN_APPROVAL_RATE);

	// QualificationRequirement qualReq = new QualificationRequirement();
	// qualReq.setQualificationTypeId(RequesterService.LOCALE_QUALIFICATION_TYPE_ID);
	// qualReq.setComparator(Comparator.EqualTo);
	// Locale country = new Locale();
	// country.setCountry("US");
	// qualReq.setLocaleValue(country);

	QualificationRequirement[] qualReqs = null;

	// QualificationRequirement qualReq1 = new QualificationRequirement();
	// qualReq1.setQualificationTypeId("2F1KVCNHMVHV8E9PBUB2A4J79LU20F"); //
	// sandbox
	// qualReq1.setQualificationTypeId("2NDP2L92HECWY8NS8H3CK0CP5L9GHO"); //
	// production
	// qualReq1.setComparator(Comparator.Exists);
	QualificationRequirement qualReq2 = new QualificationRequirement();
	qualReq2.setQualificationTypeId(RequesterService.APPROVAL_RATE_QUALIFICATION_TYPE_ID);
	qualReq2.setComparator(Comparator.GreaterThanOrEqualTo);
	qualReq2.setIntegerValue(Parameters.MIN_APPROVAL_RATE);

	if (Parameters.IS_USE_MASTER) {
	    QualificationRequirement qualReq1 = new QualificationRequirement();
	    if (Parameters.CURRENT_MODE == Parameters.PRODUCTION_MODE) {
		qualReq1.setQualificationTypeId("2NDP2L92HECWY8NS8H3CK0CP5L9GHO"); // production
	    } else {
		qualReq1.setQualificationTypeId("2F1KVCNHMVHV8E9PBUB2A4J79LU20F"); // sandbox
	    }
	    qualReq1.setComparator(Comparator.Exists);
	    qualReqs = new QualificationRequirement[] { qualReq1, qualReq2 };
	} else {
	    qualReqs = new QualificationRequirement[] { qualReq2 };
	}

	// QualificationRequirement[] qualReqs = null;
	// qualReqs = new QualificationRequirement[] { qualReq1, qualReq2 };
	// qualReqs = new QualificationRequirement[] { qualReq2 }; // only
	// minimal approval rate

	return qualReqs;
    }
}
